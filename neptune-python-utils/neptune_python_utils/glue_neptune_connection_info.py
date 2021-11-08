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
import requests
from neptune_python_utils.endpoints import Endpoints

class GlueNeptuneConnectionInfo:
    
    def __init__(self, region, role_arn):
            self.region = region
            self.role_arn = role_arn
    
    def neptune_endpoints(self, neptune_endpoint):
        parse_result = requests.utils.urlparse(neptune_endpoint)
        netloc_parts = parse_result.netloc.split(':')
        host = netloc_parts[0]
        port = netloc_parts[1]
        
        return Endpoints(neptune_endpoint=host, neptune_port=port, region_name=self.region, role_arn=self.role_arn)