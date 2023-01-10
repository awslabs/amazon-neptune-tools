# Copyright Amazon.com, Inc. or its affiliates.
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

import sys
import os
import backoff
import logging
import boto3
import itertools
import traceback
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.traversal import *
from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints
from neptune_python_utils.mappings import Mappings

logging.getLogger('backoff').addHandler(logging.StreamHandler())
logger = logging.getLogger()

GremlinUtils.init_statics(globals())

def get_cardinality(s):
    return Cardinality.single if s == 'single' else Cardinality.set_
    
def add_property_to_vertex(t, mapping, value):

    cardinality = get_cardinality(mapping.cardinality)
    if cardinality == Cardinality.set_ and mapping.is_multi_valued:
        col = value if type(value) in [list, set, tuple] else mapping.separator.split(value)      
        for s in col:
            t = t.property(cardinality, mapping.name, mapping.convert(s))
    else:
        t = t.property(cardinality, mapping.name, mapping.convert(value))
        
    return t
    
def add_property_to_edge(t, mapping, value):

    if type(value) in [list, set, tuple]:
        raise Exception('Unsupported list/set/tuple type for edge property: {}'.format(mapping.name))
    t = t.property(mapping.name, mapping.convert(value))
        
    return t
        

def add_vertex(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else mappings.get_label(row)
    
    t = t.addV(label)
    
    for key, value in row.items():
        mapping = mappings.mapping_for(key)
        if mapping.is_id_token():
            t = t.property(id, value)
        elif not mapping.is_token():
            t = add_property_to_vertex(t, mapping, value)
    return t


def upsert_vertex(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else mappings.get_label(row)
    on_upsert = kwargs.get('on_upsert', None)
    
    #updateSingleCardinalityProperties
    #updateAllProperties
    #replaceAllProperties
    
    create_traversal = __.addV(label)
    
    updateable_items = []
    
    for key, value in row.items():
        
        mapping = mappings.mapping_for(key)
        
        if mapping.is_id_token():
            create_traversal = create_traversal.property(id, value)
        elif not mapping.is_token():
            if not on_upsert:
                create_traversal = add_property_to_vertex(create_traversal, mapping, value)      
            elif on_upsert == 'updateSingleCardinalityProperties':
                if mapping.cardinality == 'single':
                    updateable_items.append((key, value))
                else:                   
                    create_traversal = add_property_to_vertex(create_traversal, mapping, value)
            elif on_upsert == 'updateAllProperties':
                updateable_items.append((key, value))
            elif on_upsert == 'replaceAllProperties':
                pass
            
    t = t.V(mappings.get_id(row)).fold().coalesce(__.unfold(), create_traversal)
    
    if updateable_items:
        for key, value in updateable_items:
            mapping = mappings.mapping_for(key)
            t = add_property_to_vertex(t, mapping, value)

    return t


def add_edge(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else mappings.get_label(row)
    
    t = t.addE(label).from_(V(mappings.get_from(row))).to(V(mappings.get_to(row))).property(id, mappings.get_id(row))
    
    for key, value in row.items():
        mapping = mappings.mapping_for(key)
        if not mapping.is_token():
            t = add_property_to_edge(t, mapping, value)
    return t


def upsert_edge(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else mappings.get_label(row)
    on_upsert = kwargs.get('on_upsert', None)
    
    #updateSingleCardinalityProperties
    #updateAllProperties
    #replaceAllProperties
    
    create_traversal = __.addE(label).from_(V(mappings.get_from(row))).to(V(mappings.get_to(row))).property(id, mappings.get_id(row))

    if not on_upsert:
        for key, value in row.items():       
            mapping = mappings.mapping_for(key)
            if not mapping.is_token():
                create_traversal = add_property_to_edge(create_traversal, mapping, value)
            
    t = t.V(mappings.get_from(row)).outE(label).hasId(mappings.get_id(row)).fold().coalesce(__.unfold(), create_traversal)
    
    if on_upsert and on_upsert in ['updateSingleCardinalityProperties', 'updateAllProperties']:
        for key, value in row.items():     
            mapping = mappings.mapping_for(key)       
            if not mapping.is_token():
                t = add_property_to_edge(t, mapping, value)
        
    
    return t

def replace_vertex_properties(t, row, **kwargs):
    mappings = kwargs['mappings']
    
    vertex_id = mappings.get_id(row)
            
    if vertex_id:
        t = t.sideEffect(V(vertex_id).properties().drop())
        t = t.V(vertex_id)
        for key, value in row.items(): 
            mapping = mappings.mapping_for(key) 
            if not mapping.is_token():
                t = add_property_to_vertex(t, mapping, value)
    return t
    
def replace_edge_properties(t, row, **kwargs):
    mappings = kwargs['mappings']
    
    edge_id = mappings.get_id(row)
    from_id = mappings.get_from(row)
    
    if edge_id and from_id:
        t = t.sideEffect(V(from_id).outE().hasId(edge_id).properties().drop())
        t = t.V(from_id).outE().hasId(edge_id)
        for key, value in row.items(): 
            mapping = mappings.mapping_for(key) 
            if not mapping.is_token():
                t = add_property_to_edge(t, mapping, value)

    return t

def add_properties_to_edge(t, row, **kwargs):
    mappings = kwargs['mappings']
    
    edge_id = mappings.get_id(row)
    from_id = mappings.get_from(row)
    label = mappings.get_label(row)
    
    if edge_id and from_id:
        t = t.V(from_id).outE(label).hasId(edge_id)
        for key, value in row.items(): 
            mapping = mappings.mapping_for(key) 
            if not mapping.is_token():
                t = add_property_to_edge(t, mapping, value)

    return t
    
reconnectable_err_msgs = [ 
  'ReadOnlyViolationException',
  'Server disconnected',
  'Connection refused',
  'Connection was closed by server',
  'Received error on read',
  'Connection was already closed'
]

retriable_err_msgs = ['ConcurrentModificationException'] + reconnectable_err_msgs

network_errors = [OSError]

retriable_errors = [GremlinServerError, RuntimeError] + network_errors   

def is_non_retriable_error(e):
    def is_retriable_error(e):
        is_retriable = False
        err_msg = str(e)
      
        if isinstance(e, tuple(network_errors)):
            is_retriable = True
        else:
            for retriable_err_msg in retriable_err_msgs:
                if retriable_err_msg in err_msg:
                    is_retriable = True
        
        print('error: [{}] {}'.format(type(e), err_msg))
        print('is_retriable: {}'.format(is_retriable))
                
        return is_retriable
    
    return not is_retriable_error(e)

def reset_connection_if_connection_issue(params):
    
    batch_utils = params['args'][0]
    
    is_reconnectable = False
    
    e = sys.exc_info()[1]
    err_msg = str(e)
    
    if isinstance(e, tuple(network_errors)):
        is_reconnectable = True
    else:
        for reconnectable_err_msg in reconnectable_err_msgs:
            if reconnectable_err_msg in err_msg:
                is_reconnectable = True
    
    print('is_reconnectable: {}, num_tries: {}'.format(is_reconnectable, params['tries']))
      
    if is_reconnectable:
        try:
            batch_utils.conn.close()
        except:
            print('[{}] Error closing connection: {}'.format(pid, traceback.format_exc()))
        finally:
            batch_utils.conn = None
            batch_utils.g = None

def publish_metrics(invocation_details):
    
    batch_utils = invocation_details['args'][0]
    number_tries = invocation_details['tries']
     
    retries = number_tries - 1
    job_name = batch_utils.job_name
    region = batch_utils.region
    
    if retries > 0:
        print('Retries: {}, Elapsed: {}s'.format(retries, invocation_details['elapsed']))
    
    if job_name and retries > 0:
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        cloudwatch.put_metric_data(
            MetricData = [
                {
                    'MetricName': 'Retries',
                    'Dimensions': [
                        {
                            'Name': 'client',
                            'Value': job_name
                        }
                    ],
                    'Unit': 'Count',
                    'Value': float(retries)
                },
            ],
            Namespace='awslabs/amazon-neptune-tools/neptune-python-utils'
        ) 

class BatchUtils:
    
    def __init__(self, endpoints, job_name=None, to_dict=lambda x: x, pool_size=1, **kwargs):
        
        self.gremlin_utils = GremlinUtils(endpoints)
        self.conn = None
        self.g = None
        self.region = endpoints.region
        self.job_name = job_name
        self.to_dict = to_dict
        self.pool_size = pool_size
        self.kwargs = kwargs
        
    def close(self):
        try:
            self.gremlin_utils.close()
        except:
            pass 
         
    def __execute_batch_internal(self, rows, operations, **kwargs):
        @backoff.on_exception(backoff.constant,
            tuple(retriable_errors),
            max_tries=5,
            giveup=is_non_retriable_error,
            on_backoff=reset_connection_if_connection_issue,
            on_success=publish_metrics,
            interval=2,
            jitter=backoff.full_jitter)
        def execute(self, rows, operations, **kwargs):
        
            if not self.conn:
                self.conn = self.gremlin_utils.remote_connection(pool_size=self.pool_size, **self.kwargs)
                self.g = self.gremlin_utils.traversal_source(connection=self.conn) 
            
            t = self.g
            for operation in operations:
                for row in rows:
                    t = operation(t, row, **kwargs)
            t.iterate()
        
        return execute(self, rows, operations, **kwargs)
        
    def execute_batch(self, rows, operations=[], batch_size=50, **kwargs):
          
        if 'mappings' not in kwargs:
            kwargs['mappings'] = Mappings()

        rows_list = []
        
        for row in rows:
            rows_list.append(self.to_dict(row))
            if len(rows_list) == batch_size:
                self.__execute_batch_internal(rows_list, operations, **kwargs)
                rows_list = []
                
        if rows_list:
            self.__execute_batch_internal(rows_list, operations, **kwargs)
 
    def add_vertices(self, batch_size=50, rows=None, **kwargs):
        def batch_op(rows):
            self.execute_batch(rows, operations=[add_vertex], batch_size=batch_size, **kwargs)
        return batch_op(rows) if rows else batch_op
        
    def upsert_vertices(self, batch_size=50, rows=None, **kwargs):
        def batch_op(rows):
            operations = [upsert_vertex]
            on_upsert = kwargs.get('on_upsert', None)
            if on_upsert and on_upsert == 'replaceAllProperties':
                operations.append(replace_vertex_properties)
            self.execute_batch(rows, operations=operations, batch_size=batch_size, **kwargs)
        return batch_op(rows) if rows else batch_op
    
    def add_edges(self, batch_size=50, rows=None, **kwargs):
        def batch_op(rows):
            self.execute_batch(rows, operations=[add_edge], batch_size=batch_size, **kwargs)
        return batch_op(rows) if rows else batch_op
        
    def upsert_edges(self, batch_size=50, rows=None, **kwargs):       
        def batch_op(rows):
            operations = [upsert_edge]
            on_upsert = kwargs.get('on_upsert', None)
            if on_upsert and on_upsert == 'replaceAllProperties':
                operations.append(replace_edge_properties)
            self.execute_batch(rows, operations=operations, batch_size=batch_size, **kwargs)
        return batch_op(rows) if rows else batch_op
    
    def add_edge_properties(self, batch_size=50, rows=None, **kwargs):
        def batch_op(rows):
            self.execute_batch(rows, operations=[add_properties_to_edge], batch_size=batch_size, **kwargs)
        return batch_op(rows) if rows else batch_op