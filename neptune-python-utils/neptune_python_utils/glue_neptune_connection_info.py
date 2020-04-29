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

import sys, boto3
from urllib.parse import urlparse
from neptune_python_utils.endpoints import Endpoints

class GlueNeptuneConnectionInfo:
    
    def __init__(self, region, role_arn):
            self.region = region
            self.role_arn = role_arn
    
    def neptune_endpoints(self, connection_name):
        """Gets Neptune endpoint information from the Glue Data Catalog.
        
        You may need to install a Glue VPC Endpoint in your VPC for this method to work.
        
        You can store Neptune endpoint information as JDBC connections in the Glue Data Catalog.
        JDBC connection strings must begin 'jdbc:'. To store a Neptune endpoint, use the following format:
        
        'jdbc:<protocol>://<dns_name>:<port>/<endpoint>'
        
        For example, if you store:
        
        'jdbc:wss://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin'
        
        – this method will return:
        
        'wss://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin' 
        
        Example:
        >>> gremlin_endpoint = GlueNeptuneConnectionInfo(glueContext).neptune_endpoint('neptune')
        """
        glue = boto3.client('glue', region_name=self.region)
        
        connection = glue.get_connection(Name=connection_name)
        neptune_uri = connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'][5:]
        parse_result = urlparse(neptune_uri)
        netloc_parts = parse_result.netloc.split(':')
        host = netloc_parts[0]
        port = netloc_parts[1]
        
        return Endpoints(neptune_endpoint=host, neptune_port=port, region_name=self.region, role_arn=self.role_arn)