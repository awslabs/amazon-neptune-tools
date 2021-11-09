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
import boto3
import requests
from neptune_python_utils.endpoints import Endpoints

class GlueNeptuneConnectionInfo:
    
    def __init__(self, region, role_arn):
            self.region = region
            self.role_arn = role_arn
    
    def neptune_endpoints(self, connection_name):
        """Gets Neptune endpoint information from the AWS Glue Data Catalog.
        
        You may need to install a Glue VPC Endpoint in your VPC for this method to work.
        
        You can either create a Glue Connection type of 'JDBC' or 'NETWORK'. 
        
        When you use Glue Connection Type of 'JDBC' store the Amazon Neptune endpoint in 'JDBC_CONNECTION_URL' field, e.g. 'jdbc:wss://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin'. 
        
        When you use Glue Connection Type of 'NETWORK' store the Amazon Neptune endpoint in 'Description' field, e.g. 'wss://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin'.
        
        When you invoke the method it returns Neptune endpoint, e.g. 'wss://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin' 
        
        Example:
        >>> gremlin_endpoint = GlueNeptuneConnectionInfo(glueContext).neptune_endpoint('neptune')
        """
        glue = boto3.client('glue', region_name=self.region)
        connection = glue.get_connection(Name=connection_name)['Connection']

        if connection['ConnectionType'] == "JDBC":
            neptune_uri = connection['ConnectionProperties']['JDBC_CONNECTION_URL'][5:]

        if connection['ConnectionType'] == "NETWORK":
            neptune_uri = connection['Description']

        parse_result = requests.utils.urlparse(neptune_uri)
        netloc_parts = parse_result.netloc.split(':')
        host = netloc_parts[0]
        port = netloc_parts[1]
        
        return Endpoints(neptune_endpoint=host, neptune_port=port, region_name=self.region, role_arn=self.role_arn)