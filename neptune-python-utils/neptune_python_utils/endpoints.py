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

import sys, os, base64, datetime, hashlib, hmac, boto3, urllib, uuid, threading
from botocore.credentials import Credentials
from botocore.credentials import RefreshableCredentials

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

class RequestParameters:
    
    def __init__(self, uri, querystring, headers):
        self.uri = uri
        self.querystring = querystring
        self.headers = headers

class Endpoint:
    
    def __init__(self, protocol, neptune_endpoint, neptune_port, suffix, region, credentials=None, role_arn=None): 
        
        if credentials is None and role_arn is None:
            raise Exception('You must supply either a credentials or role_arn')
        
        self.protocol = protocol
        self.neptune_endpoint = neptune_endpoint
        self.neptune_port = neptune_port
        self.suffix = suffix
        self.region = region
        self.credentials = credentials
        self.role_arn = role_arn
    
    def __str__(self):
        return self.value()
        
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
        
    def prepare_request(self, method='GET', payload='', querystring={}):
        credentials =  self._get_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        session_token = credentials.token
        
        service = 'neptune-db'
        algorithm = 'AWS4-HMAC-SHA256'
        
        request_parameters = urllib.parse.urlencode(querystring, quote_via=urllib.parse.quote)
        request_parameters = request_parameters.replace('%27','%22')
        
        canonical_querystring = self.__normalize_query_string(request_parameters)
        
        t = datetime.datetime.utcnow()
        amzdate = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')      
        canonical_headers = 'host:{}:{}\nx-amz-date:{}\n'.format(
            self.neptune_endpoint, 
            self.neptune_port, 
            amzdate)
        signed_headers = 'host;x-amz-date'
        payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()
        canonical_request = '{}\n/{}\n{}\n{}\n{}\n{}'.format(
            method,
            self.suffix, 
            canonical_querystring, 
            canonical_headers, 
            signed_headers, 
            payload_hash) 
        credential_scope = '{}/{}/{}/aws4_request'.format(
            datestamp, 
            self.region,
            service)
        string_to_sign = '{}\n{}\n{}\n{}'.format(
            algorithm,
            amzdate, 
            credential_scope, 
            hashlib.sha256(canonical_request.encode('utf-8')).hexdigest())
        signing_key = self.__get_signature_key(secret_key, datestamp, self.region, service)
        signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
        authorization_header = '{} Credential={}/{}, SignedHeaders={}, Signature={}'.format(
            algorithm, 
            access_key, 
            credential_scope, 
            signed_headers, 
            signature)
        headers = {'x-amz-date':amzdate, 'Authorization':authorization_header}
        if session_token:
            headers['x-amz-security-token'] = session_token
        return RequestParameters(
            '{}?{}'.format(self.value(), canonical_querystring) if canonical_querystring else self.value(),
            canonical_querystring,
            headers
        )
        
    def __normalize_query_string(self, query):
        kv = (list(map(str.strip, s.split("=")))
              for s in query.split('&')
              if len(s) > 0)

        normalized = '&'.join('%s=%s' % (p[0], p[1] if len(p) > 1 else '')
                              for p in sorted(kv))
        return normalized
        
    def __sign(self, key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def __get_signature_key(self, key, dateStamp, regionName, serviceName):
        kDate = self.__sign(('AWS4' + key).encode('utf-8'), dateStamp)
        kRegion = self.__sign(kDate, regionName)
        kService = self.__sign(kRegion, serviceName)
        kSigning = self.__sign(kService, 'aws4_request')
        return kSigning
        

class Endpoints:
    
    def __init__(self, neptune_endpoint=None, neptune_port=None, region_name=None, credentials=None, role_arn=None):
        
        if neptune_endpoint is None:
            assert ('NEPTUNE_CLUSTER_ENDPOINT' in os.environ), 'neptune_endpoint is missing.'
            self.neptune_endpoint = os.environ['NEPTUNE_CLUSTER_ENDPOINT']
        else:
            self.neptune_endpoint = neptune_endpoint
            
        if neptune_port is None:
            self.neptune_port = 8182 if 'NEPTUNE_CLUSTER_PORT' not in os.environ else os.environ['NEPTUNE_CLUSTER_PORT']
        else:
            self.neptune_port = neptune_port
           
        session = boto3.session.Session()
        
        self.region = region_name if region_name is not None else session.region_name
        self.role_arn = role_arn
        
        if credentials is None:
            self.credentials = session.get_credentials() if role_arn is None else None
        else:
            self.credentials = credentials
            
            
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
    
    def __endpoint(self, protocol, neptune_endpoint, neptune_port, suffix):
        return Endpoint(protocol, neptune_endpoint, neptune_port, suffix, self.region, self.credentials, self.role_arn)
  