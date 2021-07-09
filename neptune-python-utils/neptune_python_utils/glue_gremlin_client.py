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

from neptune_python_utils.batch_utils import BatchUtils

class GlueGremlinClient:
    
    def __init__(self, endpoints, job_name=None, **kwargs): 
        self.endpoints = endpoints
        self.job_name = job_name
        self.kwargs = kwargs
        
    def __execute_batch(self, f, pool_size=1):
        print('endpoints: {}'.format(self.endpoints))
        print('job_name: {}'.format(self.job_name))
        print('pool_size: {}'.format(pool_size))
        print('kwargs: {}'.format(self.kwargs))
        def execute_batch(rows):
            batch_utils = None
            try:
                batch_utils = BatchUtils(self.endpoints, job_name=self.job_name, to_dict=lambda x: x.asDict(), pool_size=pool_size, **self.kwargs)
                return f(batch_utils, rows)
            finally:
                if batch_utils:
                    batch_utils.close()
        return execute_batch        
        
    def add_vertices(self, label, batch_size=1, pool_size=1, **kwargs):
        """Adds a vertex with the supplied label for each row in a DataFrame partition.
        If the DataFrame contains an '~id' column, the values in this column will be treated as user-supplied IDs for the new vertices.
        If the DataFrame does not have an '~id' column, Neptune will autogenerate a UUID for each vertex. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.add_vertices('Product'))
        """
        
        return self.__execute_batch(lambda b, rows: b.add_vertices(batch_size=batch_size, rows=rows, label=label, **kwargs), pool_size=pool_size)        
       
        
    def upsert_vertices(self, label, batch_size=1, pool_size=1, **kwargs):
        """Conditionally adds vertices for the rows in a DataFrame partition using the Gremlin coalesce() idiom.
        The DataFrame must contain an '~id' column. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.upsert_vertices('Product'))
        """
        
        return self.__execute_batch(lambda b, rows: b.upsert_vertices(batch_size=batch_size, rows=rows, label=label, **kwargs), pool_size=pool_size) 

    def add_edges(self, label, batch_size=1, pool_size=1, **kwargs):
        """Adds an edge with the supplied label for each row in a DataFrame partition.
        If the DataFrame contains an '~id' column, the values in this column will be treated as user-supplied IDs for the new edges.
        If the DataFrame does not have an '~id' column, Neptune will autogenerate a UUID for each edge. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.add_edges('ORDER_DETAIL'))
        """
        
        return self.__execute_batch(lambda b, rows: b.add_edges(batch_size=batch_size, rows=rows, label=label, **kwargs), pool_size=pool_size) 
        
    def upsert_edges(self, label, batch_size=1, pool_size=1, **kwargs):
        """Conditionally adds edges for the rows in a DataFrame partition using the Gremlin coalesce() idiom.
        The DataFrame must contain '~id', '~from', '~to' and '~label' columns. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.upsert_edges('ORDER_DETAIL'))
        """
        
        return self.__execute_batch(lambda b, rows: b.upsert_edges(batch_size=batch_size, rows=rows, label=label, **kwargs), pool_size=pool_size) 
        
        