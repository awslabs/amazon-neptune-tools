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
import backoff
import logging
import boto3
import itertools
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

def add_vertex(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else row['~label']
    
    t = t.addV(label)
    
    for key, value in row.items():
        mapping = mappings.mapping_for(key)
        if mapping.name == '~id':
            t = t.property(id, value)
        elif mapping.name == '~label':
            pass
        else:
            t = t.property(mapping.name, mapping.convert(value))
    return t


def upsert_vertex(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else row['~label']
    on_upsert = kwargs.get('on_upsert', None)
    
    #updateSingleCardinalityProperties
    #updateAllProperties
    #replaceAllProperties
    
    create_traversal = __.addV(label)
    
    updateable_items = []
    
    for key, value in row.items():
        
        mapping = mappings.mapping_for(key)
        
        if mapping.name == '~id':
            create_traversal = create_traversal.property(id, value)
        elif mapping.name == '~label':
            pass
        else:
            if not on_upsert:
                create_traversal = create_traversal.property(mapping.name, mapping.convert(value))              
            elif on_upsert == 'updateSingleCardinalityProperties':
                if mapping.cardinality == 'single':
                    updateable_items.append((key, value))
                else:                   
                    create_traversal = create_traversal.property(get_cardinality(mapping.cardinality), mapping.name, mapping.convert(value))
            elif on_upsert == 'updateAllProperties':
                updateable_items.append((key, value))
            elif on_upsert == 'replaceAllProperties':
                pass
            
    t = t.V(row['~id']).fold().coalesce(__.unfold(), create_traversal)
    
    if updateable_items:
        for key, value in updateable_items:
            mapping = mappings.mapping_for(key)
            t = t.property(get_cardinality(mapping.cardinality), mapping.name, mapping.convert(value))

    return t


def add_edge(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else row['~label']
    
    t = t.addE(label).from_(V(row['~from'])).to(V(row['~to'])).property(id, row['~id'])
    
    for key, value in row.items():
        mapping = mappings.mapping_for(key)
        if mapping.name not in ['~id', '~from', '~to', '~label']:
            t = t.property(mapping.name, mapping.convert(value))
    return t


def upsert_edge(t, row, **kwargs):
    
    mappings = kwargs['mappings']
    label = kwargs['label'] if 'label' in kwargs else row['~label']
    on_upsert = kwargs.get('on_upsert', None)
    
    create_traversal = __.addE(label).from_(V(row['~from'])).to(V(row['~to'])).property(id, row['~id'])
    
    updateable_items = []
    
    for key, value in row.items():
        
        mapping = mappings.mapping_for(key)
        
        if mapping.name not in ['~id', '~from', '~to', '~label']:
            create_traversal.property(mapping.name, mapping.convert(value))
            
    t = t.V(row['~from']).outE(label).hasId(row['~id']).fold().coalesce(__.unfold(), create_traversal)
    
    return t

def replace_vertex_properties(t, row, **kwargs):
    mappings = kwargs['mappings']
    
    vertex_id = None
    
    for key, value in row.items():      
        mapping = mappings.mapping_for(key)       
        if mapping.name == '~id':
            vertex_id = value
            t = t.sideEffect(V(vertex_id).properties().drop())
            
    if vertex_id:
        t = t.V(vertex_id)
        for key, value in row.items(): 
            mapping = mappings.mapping_for(key) 
            if mapping.name == '~id':
                pass
            elif mapping.name == '~label':
                pass
            else:
                t = t.property(get_cardinality(mapping.cardinality), mapping.name, mapping.convert(value))
    return t

    

class BatchUtils:
    
    def __init__(self, endpoints, job_name=None, to_dict=lambda x: x):
        
        self.gremlin_utils = GremlinUtils(endpoints)
        self.region = endpoints.region
        self.job_name = job_name
        self.to_dict = to_dict
        
    def publish_metrics(invocation_details):
        
        number_tries = invocation_details['tries']
        retries = number_tries - 1
        kwargs = invocation_details['kwargs']
        job_name = kwargs['job_name']
        region = kwargs['region']
        
        if retries > 0:
            logger.info('Retries: {}, Elapsed: {}s'.format(retries, invocation_details['elapsed']))
        
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
                Namespace='awslab/amazon-neptune-tools/neptune-python-utils'
            )
        
        
    def not_cme(e):
        return '"code":"ConcurrentModificationException"' not in str(e)
     
    @backoff.on_exception(backoff.expo,
                          GremlinServerError,
                          max_tries=5,
                          giveup=not_cme,
                          on_success=publish_metrics,
                          on_giveup=publish_metrics)
    def retry_query(self, **kwargs):      
        q = kwargs['query']
        q.next()
        
    def __execute_batch_internal(self, conn, t, rows, operations, **kwargs):
        for operation in operations:
            for row in rows:
                t = operation(t, row, **kwargs)
        self.retry_query(query=t, job_name=self.job_name, region=self.region)
        
    def execute_batch(self, rows, operations=[], batch_size=50, **kwargs):
          
        conn = self.gremlin_utils.remote_connection()
        
        if 'mappings' not in kwargs:
            kwargs['mappings'] = Mappings()
        
        try:
            g = self.gremlin_utils.traversal_source(connection=conn)           
            
            rows_list = []
            
            for row in rows:
                rows_list.append(self.to_dict(row))
                if len(rows_list) == batch_size:
                    self.__execute_batch_internal(conn, g, rows_list, operations, **kwargs)
                    rows_list = []
                    
            if rows_list:
                self.__execute_batch_internal(conn, g, rows_list, operations, **kwargs)
            
        finally:
            conn.close()
 
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
            self.execute_batch(rows, operations=[upsert_edge], batch_size=batch_size, **kwargs)
        return batch_op(rows) if rows else batch_op