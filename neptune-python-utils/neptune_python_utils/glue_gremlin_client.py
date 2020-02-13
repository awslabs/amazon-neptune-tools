# Copyright 2020 Amazon.com, Inc. or its affiliates.
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

from pyspark.sql.functions import lit
from pyspark.sql.functions import format_string
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.traversal import *
from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints
    
class GlueGremlinClient:
    
    def __init__(self, endpoints):
        
        self.gremlin_utils = GremlinUtils(endpoints)
        
        GremlinUtils.init_statics(globals())

        
    def add_vertices(self, label):
        """Adds a vertex with the supplied label for each row in a DataFrame partition.
        If the DataFrame contains an '~id' column, the values in this column will be treated as user-supplied IDs for the new vertices.
        If the DataFrame does not have an '~id' column, Neptune will autogenerate a UUID for each vertex. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.add_vertices('Product'))
        """
        def add_vertices_for_label(rows):
            try:
                conn = self.gremlin_utils.remote_connection()
                g = self.gremlin_utils.traversal_source(connection=conn) 
                for row in rows:
                    entries = row.asDict()
                    traversal = g.addV(label)
                    for key, value in entries.items():
                        key = key.split(':')[0]
                        if key == '~id':
                            traversal.property(id, value)
                        elif key == '~label':
                            pass
                        else:
                            traversal.property(key, value)
                    traversal.next() 
                conn.close()
            except GremlinServerError as err:
                print("Neptune error: {0}".format(err))
            except:
                print("Unexpected error:", sys.exc_info()[0])
        return add_vertices_for_label
        
    def upsert_vertices(self, label):
        """Conditionally adds vertices for the rows in a DataFrame partition using the Gremlin coalesce() idiom.
        The DataFrame must contain an '~id' column. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.upsert_vertices('Product'))
        """
        def upsert_vertices_for_label(rows):
            try:
                conn = self.gremlin_utils.remote_connection()
                g = self.gremlin_utils.traversal_source(connection=conn) 
                for row in rows:
                    entries = row.asDict()
                    create_traversal = __.addV(label)
                    for key, value in entries.items():
                        key = key.split(':')[0]
                        if key == '~id':
                            create_traversal.property(id, value)
                        elif key == '~label':
                            pass
                        else:
                            create_traversal.property(key, value)
                    g.V(entries['~id']).fold().coalesce(__.unfold(), create_traversal).next() 
                conn.close()
            except GremlinServerError as err:
                print("Neptune error: {0}".format(err))
            except:
                print("Unexpected error:", sys.exc_info()[0])
        return upsert_vertices_for_label
    
    def add_edges(self, label):
        """Adds an edge with the supplied label for each row in a DataFrame partition.
        If the DataFrame contains an '~id' column, the values in this column will be treated as user-supplied IDs for the new edges.
        If the DataFrame does not have an '~id' column, Neptune will autogenerate a UUID for each edge. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.add_edges('ORDER_DETAIL'))
        """
        def add_edges_for_label(rows):
            try:
                conn = self.gremlin_utils.remote_connection()
                g = self.gremlin_utils.traversal_source(connection=conn) 
                for row in rows:
                    entries = row.asDict()
                    traversal = g.V(row['~from']).addE(label).to(V(row['~to'])).property(id, row['~id'])
                    for key, value in entries.items():
                        key = key.split(':')[0]
                        if key not in ['~id', '~from', '~to', '~label']:
                            traversal.property(key, value)
                    traversal.next() 
                conn.close()
            except GremlinServerError as err:
                print("Neptune error: {0}".format(err))
            except:
                print("Unexpected error:", sys.exc_info()[0])
        return add_edges_for_label
        
    def upsert_edges(self, label):
        """Conditionally adds edges for the rows in a DataFrame partition using the Gremlin coalesce() idiom.
        The DataFrame must contain '~id', '~from', '~to' and '~label' columns. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.upsert_edges('ORDER_DETAIL'))
        """
        def add_edges_for_label(rows):
            try:
                conn = self.gremlin_utils.remote_connection()
                g = self.gremlin_utils.traversal_source(connection=conn) 
                for row in rows:
                    entries = row.asDict()
                    create_traversal = __.V(row['~from']).addE(label).to(V(row['~to'])).property(id, row['~id'])
                    for key, value in entries.items():
                        key = key.split(':')[0]
                        if key not in ['~id', '~from', '~to', '~label']:
                            create_traversal.property(key, value)
                    g.E(entries['~id']).fold().coalesce(__.unfold(), create_traversal).next()
                conn.close()
            except GremlinServerError as err:
                print("Neptune error: {0}".format(err))
            except:
                print("Unexpected error:", sys.exc_info()[0])
        return add_edges_for_label