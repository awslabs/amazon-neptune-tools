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
import os
import boto3
import uuid
import threading
import typing
from botocore.credentials import RefreshableCredentials
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from typing import Tuple, Iterable


def synchronized_method(method):
    
    outer_lock = threading.Lock()
    lock_name = "__"+method.__name__+"_lock"+"__"
    
    def sync_method(self, *args, **kws):
        with outer_lock:
            if not hasattr(self, lock_name): setattr(self, lock_name, threading.Lock())
            lock = getattr(self, lock_name)
            with lock:
                return method(self, *args, **kws)  

    return sync_method

class LazyHttpHeaders:
    
    def __init__(self, lazy_headers):
        self.lazy_headers = lazy_headers
    
    def get_all(self) -> Iterable[Tuple[str, str]]:
        return self.items()
        
    def items(self):
        return self.lazy_headers().items()
        
    def __iter__(self):
        return iter(self.items())   
        
class RequestParameters:
    
    def __init__(self, uri, querystring, headers):
        self.uri = uri
        self.querystring = querystring
        self.headers = headers

class Endpoint:
    
    def __init__(self, protocol, neptune_endpoint, neptune_port, suffix, region, credentials=None, role_arn=None, proxy_dns=None, proxy_port=8182, remove_host_header=False): 
        
        self.protocol = protocol
        self.neptune_endpoint = neptune_endpoint
        self.neptune_port = neptune_port
        self.suffix = suffix
        self.region = region
        self.proxy_dns = proxy_dns
        self.proxy_port = proxy_port
        self.remove_host_header = remove_host_header
        
        if role_arn:
            self.role_arn = role_arn
            self.credentials = None
        elif credentials:
            self.role_arn = None
            self.credentials = credentials
        else:
            self.role_arn = None
            self.credentials = self._get_session_credentials()
    
    def __str__(self):
        return self.value()
        
    def _get_session_credentials(self):
        session = boto3.session.Session()
        return session.get_credentials();       
        
    @synchronized_method
    def _get_credentials(self):        

        if self.credentials is None:
            sts = boto3.client('sts', region_name=self.region)
            
            role = sts.assume_role(
                RoleArn=self.role_arn,
                RoleSessionName=uuid.uuid4().hex,
                DurationSeconds=3600
            )
            
            self.credentials = RefreshableCredentials.create_from_metadata(
                metadata=self._new_credentials(),
                refresh_using=self._new_credentials,
                method="sts-assume-role",
            )
        
        return self.credentials.get_frozen_credentials()
               
    def _new_credentials(self):
        sts = boto3.client('sts', region_name=self.region)
        
        role = sts.assume_role(
            RoleArn=self.role_arn,
            RoleSessionName=uuid.uuid4().hex,
            DurationSeconds=3600
        )
        
        return {
            'access_key': role['Credentials']['AccessKeyId'],
            'secret_key': role['Credentials']['SecretAccessKey'], 
            'token': role['Credentials']['SessionToken'],
             'expiry_time': role['Credentials']['Expiration'].isoformat()
        }
        
    def value(self):
        return '{}://{}:{}/{}'.format(self.protocol, self.neptune_endpoint, self.neptune_port, self.suffix)
     
    def __proxied_neptune_endpoint_url(self):
        if self.proxy_dns:
            return '{}://{}:{}/{}'.format(self.protocol, self.proxy_dns, self.proxy_port, self.suffix)
        else:
            return '{}://{}:{}/{}'.format(self.protocol, self.neptune_endpoint, self.neptune_port, self.suffix)
        
        
    def prepare_request(self, method='GET', payload=None, querystring={}, headers={}):
        
        def get_headers():
            
            service = 'neptune-db'
            
            if 'host' not in headers and 'Host' not in headers:
                headers['Host'] = self.neptune_endpoint

            request = AWSRequest(method=method, url=self.__proxied_neptune_endpoint_url(), headers=headers, data=payload, params=querystring)
        
            SigV4Auth(self._get_credentials(), service, self.region).add_auth(request)
            
            signed_headers = {}
            
            for k,v in request.headers.items():
                signed_headers[k] = v
            
            if self.remove_host_header:
                if 'host' in signed_headers:
                    signed_headers.pop('host')
                if 'Host' in signed_headers:
                    signed_headers.pop('Host')
            
            return signed_headers
            
        return RequestParameters(
            self.__proxied_neptune_endpoint_url(),
            None,
            LazyHttpHeaders(get_headers)
        )
        

class Endpoints:
    
    def __init__(self, neptune_endpoint=None, neptune_port=None, region_name=None, credentials=None, role_arn=None, proxy_dns=None, proxy_port=8182, remove_host_header=False):
        
        if neptune_endpoint is None:
            assert ('NEPTUNE_CLUSTER_ENDPOINT' in os.environ), 'neptune_endpoint is missing.'
            self.neptune_endpoint = os.environ['NEPTUNE_CLUSTER_ENDPOINT']
        else:
            self.neptune_endpoint = neptune_endpoint
            
        if neptune_port is None:
            self.neptune_port = 8182 if 'NEPTUNE_CLUSTER_PORT' not in os.environ else os.environ['NEPTUNE_CLUSTER_PORT']
        else:
            self.neptune_port = neptune_port
            
        if region_name:
            self.region = region_name
        else:
            session = boto3.session.Session()
            self.region = session.region_name
            
        self.credentials = credentials
        self.role_arn = role_arn
        self.proxy_dns = proxy_dns
        self.proxy_port = proxy_port
        self.remove_host_header = remove_host_header
            
            
    def gremlin_endpoint(self):
        return self.__endpoint('wss', self.neptune_endpoint, self.neptune_port, 'gremlin')
    
    def sparql_endpoint(self):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'sparql')
    
    def loader_endpoint(self):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'loader')
    
    def load_status_endpoint(self, load_id):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'loader/{}'.format(load_id))
        
    def status_endpoint(self):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'status')
        
    def gremlin_stream_endpoint(self):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'gremlin/stream')
        
    def sparql_stream_endpoint(self):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'sparql/stream')
        
    def propertygraph_stream_endpoint(self):
        return self.__endpoint('https', self.neptune_endpoint, self.neptune_port, 'pg/stream')
    
    def __endpoint(self, protocol, neptune_endpoint, neptune_port, suffix):
        return Endpoint(protocol, neptune_endpoint, neptune_port, suffix, self.region, self.credentials, self.role_arn, self.proxy_dns, self.proxy_port, self.remove_host_header)
  