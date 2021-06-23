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
    
    def __init__(self, endpoints, job_name=None):        
        self.batch_utils = BatchUtils(endpoints, job_name=job_name, to_dict=lambda x: x.asDict())
 
    def add_vertices(self, label, batch_size=1):
       """Adds a vertex with the supplied label for each row in a DataFrame partition.
       If the DataFrame contains an '~id' column, the values in this column will be treated as user-supplied IDs for the new vertices.
       If the DataFrame does not have an '~id' column, Neptune will autogenerate a UUID for each vertex. 
       
       Example:
       >>> dynamicframe.toDF().foreachPartition(neptune.add_vertices('Product'))
       """
       return self.batch_utils.add_vertices(batch_size=batch_size, label=label)
        
    def upsert_vertices(self, label, batch_size=1):
        """Conditionally adds vertices for the rows in a DataFrame partition using the Gremlin coalesce() idiom.
        The DataFrame must contain an '~id' column. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.upsert_vertices('Product'))
        """
        return self.batch_utils.upsert_vertices(batch_size=batch_size, label=label)

    def add_edges(self, label, batch_size=1):
        """Adds an edge with the supplied label for each row in a DataFrame partition.
        If the DataFrame contains an '~id' column, the values in this column will be treated as user-supplied IDs for the new edges.
        If the DataFrame does not have an '~id' column, Neptune will autogenerate a UUID for each edge. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.add_edges('ORDER_DETAIL'))
        """
        return self.batch_utils.add_edges(batch_size=batch_size, label=label)
        
    def upsert_edges(self, label, batch_size=1):
        """Conditionally adds edges for the rows in a DataFrame partition using the Gremlin coalesce() idiom.
        The DataFrame must contain '~id', '~from', '~to' and '~label' columns. 
        
        Example:
        >>> dynamicframe.toDF().foreachPartition(neptune.upsert_edges('ORDER_DETAIL'))
        """
        return self.batch_utils.upsert_edges(batch_size=batch_size, label=label)
        