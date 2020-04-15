# Copyright 2019 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

'''
@author:     krlawrence
@copyright:  Amazon.com, Inc. or its affiliates
@license:    Apache2
@contact:    @krlawrence
@deffield    created:  2019-04-02

This code uses Gremlin Python to drop an entire graph.

It is intended as an example of a multi-threaded strategy for dropping vertices and edges.

The following overall strategy is currently used.

  1. Fetch all edge IDs
     - Edges are fetched using multiple threads in large batches.
     - Smaller slices are queued up for worker threads to drop.
  2. Drop all edges using those IDs
     - Worker threads read the slices of IDs from the queue and drop the edges.
  3. Fetch all vertex IDs
     - Vertices are fetched using multiple threads in large batches.
     - Smaller slices are queued up for worker threads to drop.
  4. Drop all vertices using the fetched IDs
     - Worker threads read the slices of IDs from the queue and drop the vertices.

NOTES:
  1: To avoid possible concurrent write exceptions no fetching and dropping is done in parallel.
  2: Edges are explicitly dropped before vertices, again to avoid any conflicting writes.
  3: This code uses an in-memory, thread-safe  queue. The amount of data that can be processed
     will depend upon how big of an in-memory queue can be created. It has been tested using a 
     graph containing 10M vertices and 10M edges.
  4: While the code as written deletes an entire graph, it could be easily adapted to delete part
     of a graph instead.
  5: The following environment variables should be defined before this code is run.
     NEPTUNE_PORT    - The port that the Neptune endpoint is listening on such as 8182.
     NEPTUNE_WRITER  - The Neptune Cluster endpoint name such as
                        "mygraph.cluster-abcdefghijkl.us-east-1.neptune.amazonaws.com"
  6: This script assumes that the 'gremlinpyton' library has already been installed.
  7: For massive graphs (with hundreds of millions or billions of elements) creating a new
     Neptune cluster will be faster than trying to delete everything programmatically.

STILL TODO:
The code could be further improved by offering an option to only drop the edges and by
removing the need to count all edges and all vertices before starting work.  The use of
threads could be further optimized in future to get more reuse out of the fetcher threads.
One further refinement that would enable very large graphs to be dropped, would be to
avoid the need to read all elementment IDs into memory before dropping can start by doing
that process iteratively.  This script should probably also been turned into a class.
'''

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import *
from threading import Thread
from queue import Queue
import threading 
import time
import math
import os

# The fetch size and batch sizes should not need to be changed but can be if necessary.
# As a guide, the number of threads should be twice the number of vCPU available of the Neptune write master node.

MAX_FETCH_SIZE  =  50000  # Maximum number of IDs to fetch at a time. A large number limits the number of range() calls
EDGE_BATCH_SIZE =    500  # Number of edges to drop in each call to drop(). This affects the queue entry size.
VERTEX_BATCH_SIZE =  500  # Number of vertices to drop in each call to drop(). This affects the queue entry size. 
MAX_FETCHERS    =      8  # Maximum number of threads allowed to be created for fetching vertices and edges
NUM_THREADS     =      8  # Number of local workers to create to process the drop queue.
POOL_SIZE       =      8  # Python driver default is 4. Change to create a bigger pool.
MAX_WORKERS     =      8  # Python driver default is 5 * number of CPU on client machine.

# Ready flag is used to tell workers they can start processing the queue
ready_flag = threading.Event()

# The wait queues are used to make sure all threads have finished fetching before the 
# workers start processing the IDs to be dropped.
edge_fetch_wait_queue = Queue()
vertex_fetch_wait_queue = Queue()

# Queue that will contain the node and edge IDs that need to be dropped
pending_work = Queue()

# Location of the Neptune endpoint and port
writer = os.environ["NEPTUNE_WRITER"]
port = os.environ["NEPTUNE_PORT"]
neptune_gremlin_endpoint = 'wss://' + writer + ':' + port + '/gremlin'

# Obtain a graph traversal source for the remote endpoint.
graph=Graph()
connection = DriverRemoteConnection(neptune_gremlin_endpoint,'g',pool_size=POOL_SIZE,max_workers=MAX_WORKERS)
g = graph.traversal().withRemote(connection)


####################################################################################
# fetch_edges
#
# Calculate how many threads are needed to fetch the edge IDs and create the threads
####################################################################################
def fetch_edges(q):
    print("\nPROCESSING EDGES")
    print("Assessing number of edges.")
    count = g.E().count().next()
    print(count, "edges to drop")
    if count > 0:
        fetch_size = MAX_FETCH_SIZE
        num_threads = min(math.ceil(count/fetch_size),MAX_FETCHERS)
        bracket_size = math.ceil(count/num_threads)
        print("Will use", num_threads, "threads.")
        print("Each thread will queue", bracket_size)
        print("Queueing  IDs")

        start_offset = 0

        fetchers = [None] * num_threads

        for i in range(num_threads):
            edge_fetch_wait_queue.put(i)
            fetchers[i] = Thread(target=edge_fetcher, args=(pending_work,start_offset,bracket_size,))
            fetchers[i].setDaemon(True)
            fetchers[i].start()
            start_offset += bracket_size
        return count

####################################################################################
# fetch_vertices
#
# Calculate how many threads are needed to fetch the node IDs and create the threads
####################################################################################
def fetch_vertices(q):
    print("\nPROCESSING VERTICES")
    print("Assessing number of vertices.")
    count = g.V().count().next()
    print(count, "vertices to drop")
    if count > 0:
        fetch_size = MAX_FETCH_SIZE
        num_threads = min(math.ceil(count/fetch_size),MAX_FETCHERS)
        bracket_size = math.ceil(count/num_threads)
        print("Will use", num_threads, "threads.")
        print("Each thread will queue", bracket_size)
        print("Queueing  IDs")

        start_offset = 0

        fetchers = [None] * num_threads

        for i in range(num_threads):
            vertex_fetch_wait_queue.put(i)
            fetchers[i] = Thread(target=vertex_fetcher, args=(pending_work,start_offset,bracket_size,))
            fetchers[i].setDaemon(True)
            fetchers[i].start()
            start_offset += bracket_size
        return count

####################################################################################
# edge_fetcher
#
# Fetch edges in large batches and queue them up for deletion in smaller slices
####################################################################################
def edge_fetcher(q,start_offset,bracket_size):
    p1 = start_offset
    inc = min(bracket_size,MAX_FETCH_SIZE)
    p2 = start_offset + inc
    org = p1
    done = False
    nm = threading.currentThread().name
    print(nm,"[edges] Fetching from offset", start_offset, "with end at",start_offset+bracket_size)
    edge_fetch_wait_queue.get()

    done = False
    while not done:
        success = False
        while not success:
            print(nm,"[edges] retrieving range ({},{} batch=size={})".format(p1,p2,p2-p1))
            try:
                edges = g.E().range(p1,p2).id().toList()
                success = True
            except:
                print("***",nm,"Exception while fetching. Retrying.")
                time.sleep(1)

        slices = math.ceil(len(edges)/EDGE_BATCH_SIZE)
        s1 = 0
        s2 = 0
        for i in range(slices):
            s2 += min(len(edges)-s1,EDGE_BATCH_SIZE)
            q.put(["edges",edges[s1:s2]])
            s1 = s2
        p1 += inc
        if p1 >= org + bracket_size:
            done = True
        else:
            p2 += min(inc, org+bracket_size - p2) 
    size = q.qsize()        
    print(nm,"[edges] work done. Queue size ==>",size)
    edge_fetch_wait_queue.task_done()
    return

####################################################################################
# vertex_fetcher
#
# Fetch vertices in large batches and queue them up for deletion in smaller slices
####################################################################################
def vertex_fetcher(q,start_offset,bracket_size):
    p1 = start_offset
    inc = min(bracket_size,MAX_FETCH_SIZE)
    p2 = start_offset + inc
    org = p1
    done = False
    nm = threading.currentThread().name
    print(nm,"[vertices] Fetching from offset", start_offset, "with end at",start_offset+bracket_size)
    vertex_fetch_wait_queue.get()

    done = False
    while not done:
        success = False
        while not success:
            print(nm,"[vertices] retrieving range ({},{} batch=size={})".format(p1,p2,p2-p1))
            try:
                vertices = g.V().range(p1,p2).id().toList()
                success = True
            except:
                print("***",nm,"Exception while fetching. Retrying.")
                time.sleep(1)

        slices = math.ceil(len(vertices)/VERTEX_BATCH_SIZE)
        s1 = 0
        s2 = 0
        for i in range(slices):
            s2 += min(len(vertices)-s1,VERTEX_BATCH_SIZE)
            q.put(["vertices",vertices[s1:s2]])
            s1 = s2
        p1 += inc
        if p1 >= org + bracket_size:
            done = True
        else:
            p2 += min(inc, org+bracket_size - p2) 
    size = q.qsize()        
    print(nm,"[vertices] work done. Queue size ==>",size)
    vertex_fetch_wait_queue.task_done()
    return

####################################################################################
# worker
#
# Worker task that will handle deletion of IDs that are in the queue. Multiple workers
# will be created based on the value specified for NUM_THREADS. 
####################################################################################
def worker(q):
    c = 0
    nm = threading.currentThread().name
    print("Worker", nm, "started")
    while True:
        ready = ready_flag.wait()
        if not q.empty():
            work = q.get()
            successful = False
            while not successful:
                try:
                    if len(work[1]) > 0:
                        print("[{}] {} deleting {} {}".format(c,nm,len(work[1]), work[0]))
                        if work[0] == "edges":
                            g.E(work[1]).drop().iterate()
                        else:
                            g.V(work[1]).drop().iterate()
                    successful = True
                except:
                    # A concurrent modification error can occur if we try to drop an element
                    # that is already loacked by some other process accessing the graph.
                    # If that happens sleep briefly and try again.
                    print("{} Exception dropping some {} will retry".format(nm,work[0]))
                    print(sys.exc_info()[0])
                    print(sys.exc_info()[1])
                    time.sleep(1)
                c += 1
            q.task_done()


####################################################################################
# Do the work!
#
####################################################################################
# Fetch the edges
equeue_start_time = time.time()
ecount = fetch_edges(pending_work)
edge_fetch_wait_queue.join()
equeue_end_time = time.time()

# Create the pool of workers that will drop the edges and vertices
print("Creating drop() workers")

workers = [None] * NUM_THREADS
ready_flag.set()

edrop_start_time = time.time()
for i in range(NUM_THREADS):
    workers[i] = Thread(target=worker, args=(pending_work,))
    workers[i].setDaemon(True)
    workers[i].start()

# Wait until all of the edges in the queue have been dropped
pending_work.join()   
edrop_end_time = time.time()

# Tell the workers to wait until the vertex IDs have all been enqueued
ready_flag.clear()

# Fetch the vertex IDs
vqueue_start_time = time.time()
vcount = fetch_vertices(pending_work)
vertex_fetch_wait_queue.join()
vqueue_end_time = time.time()

# Tell the workers to start dropping the vertices
vdrop_start_time = time.time()
ready_flag.set()
pending_work.join()   
vdrop_end_time = time.time()

# Calculate how long each phase took
eqtime = equeue_end_time - equeue_start_time
vqtime = vqueue_end_time - vqueue_start_time
etime =  edrop_end_time - edrop_start_time
vtime =  vdrop_end_time - vdrop_start_time

print("Summary")
print("-------")
print("Worker threads", NUM_THREADS)
print("Max fetch size", MAX_FETCH_SIZE)
print("Edge batch size", EDGE_BATCH_SIZE)
print("Vertex batch size", VERTEX_BATCH_SIZE)
print("Edges dropped", ecount)
print("Vertices dropped", vcount)
print("Time taken to queue edges", eqtime)
print("Time taken to drop edges", etime)
print("Time taken to queue vertices", vqtime)
print("Time taken to drop vertices", vtime)

print("TOTAL TIME",eqtime + vqtime + etime + vtime)

connection.close()



