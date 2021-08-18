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

import json
import requests
import os
import sys
import time
from neptune_python_utils.endpoints import Endpoints

class BulkLoad:
    
    def __init__(self, 
        source, 
        format='csv', 
        role=None, 
        mode='AUTO',
        region=None, 
        fail_on_error=False, 
        parallelism='OVERSUBSCRIBE',
        base_uri='http://aws.amazon.com/neptune/default',
        named_graph_uri='http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph',
        update_single_cardinality_properties=False,
        queue_request=False,
        dependencies=[],
        endpoints=None,
        **kwargs):
        
        self.source = source
        self.format = format
        
        if role is None:
            assert ('NEPTUNE_LOAD_FROM_S3_ROLE_ARN' in os.environ), 'role is missing.'
            self.role = os.environ['NEPTUNE_LOAD_FROM_S3_ROLE_ARN']
        else:
            self.role = role
            
        self.mode = mode
            
        if region is None:
            assert ('AWS_REGION' in os.environ or 'SERVICE_REGION' in os.environ), 'region is missing.'
            self.region = os.environ.get('SERVICE_REGION', os.environ.get('AWS_REGION', None))
        else:
            self.region = region
        
        if endpoints is None:
            self.endpoints = Endpoints(region_name=region)
        else:
            self.endpoints = endpoints
            
        self.fail_on_error = 'TRUE' if fail_on_error else 'FALSE'
        self.parallelism = parallelism
        self.base_uri = base_uri
        self.named_graph_uri = named_graph_uri
        self.update_single_cardinality_properties = 'TRUE' if update_single_cardinality_properties else 'FALSE'
        self.queue_request = 'TRUE' if queue_request else 'FALSE'
        self.dependencies = dependencies
        self.kwargs = kwargs
            
    def __load_from(self, source):
        return { 
              'source' : source, 
              'format' : self.format,  
              'iamRoleArn' : self.role, 
              'mode': self.mode,
              'region' : self.region, 
              'failOnError' : self.fail_on_error,
              'parallelism' : self.parallelism,
              'parserConfiguration': {
                  'baseUri': self.base_uri,
                  'namedGraphUri': self.named_graph_uri
              },
              'updateSingleCardinalityProperties': self.update_single_cardinality_properties,
              'queueRequest': self.queue_request,
              'dependencies': self.dependencies
            }
    
    def __load(self, loader_endpoint, data):  
        
        json_string = json.dumps(data)
        
        request_parameters = loader_endpoint.prepare_request('POST', json_string, headers={'Content-Type':'application/json'})
        
        response = requests.post(request_parameters.uri, data=json_string, headers=request_parameters.headers, **self.kwargs)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            raise Exception('{}: {}'.format(response.status_code, response.text))
        
        json_response = response.json()
        
        return json_response['payload']['loadId']
    
    def load_async(self):
        localised_source = self.source.replace('${AWS_REGION}', self.region)
        loader_endpoint = self.endpoints.loader_endpoint()
        json_payload = self.__load_from(localised_source)
        print('''curl -X POST \\
    -H 'Content-Type: application/json' \\
    {} -d \'{}\''''.format(loader_endpoint, json.dumps(json_payload, indent=4)))
        load_id = self.__load(loader_endpoint, json_payload)
        return BulkLoadStatus(self.endpoints.load_status_endpoint(load_id), **self.kwargs)
    
    def load(self, interval=2):
        status = self.load_async()
        print('status_uri: {}'.format(status.load_status_endpoint))
        status.wait(interval)

class BulkLoadStatus:
    
    def __init__(self, load_status_endpoint, **kwargs):
        self.load_status_endpoint = load_status_endpoint
        self.kwargs = kwargs
        
    def status(self, details=False, errors=False, page=1, errors_per_page=10):
        params = {
            'errors': 'TRUE' if errors else 'FALSE', 
            'details': 'TRUE' if details else 'FALSE',
            'page': page,
            'errorsPerPage': errors_per_page
        }
        request_parameters = self.load_status_endpoint.prepare_request(querystring=params)
        
        response = requests.get(request_parameters.uri, params=params, headers=request_parameters.headers, **self.kwargs)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            raise Exception('{}: {}'.format(response.status_code, response.text))
            
        json_response = response.json()

        status = json_response['payload']['overallStatus']['status']
        
        return (status, json_response)
        
    
    def uri(self):
        return self.load_status_endpoint
    
    def wait(self, interval=2):
        while True:
            status, json_response = self.status()
            if status == 'LOAD_COMPLETED':
                print('load completed')
                break
            if status == 'LOAD_IN_PROGRESS':
                print('loading... {} records inserted'.format(json_response['payload']['overallStatus']['totalRecords']))
                time.sleep(interval)
            else:
                raise Exception(json_response)