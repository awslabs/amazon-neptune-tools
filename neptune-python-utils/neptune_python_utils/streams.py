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

import requests
import os
import time
from neptune_python_utils.endpoints import Endpoints

class EventId:

    def __init__(self, commit_num=1, op_num=1):
        
        assert (commit_num and commit_num >= 1), 'commit_num must be >= 1.'
        assert (op_num and op_num >= 1), 'op_num must be >= 1.'
        
        self.commit_num = commit_num
        self.op_num = op_num
        
    def to_dict(self):
        return { 'commitNum': self.commit_num, 'opNum': self.op_num}

class NeptuneStreamToken:

    @staticmethod
    def trim_horizon():
        return NeptuneStreamToken()

    @staticmethod
    def latest():
        return NeptuneStreamToken(iterator='LATEST')
        
    @staticmethod
    def at(event_id):
        return NeptuneStreamToken(last_event_id=event_id, iterator='AT_SEQUENCE_NUMBER')
        
    @staticmethod
    def after(event_id):
        return NeptuneStreamToken(last_event_id=event_id, iterator='AFTER_SEQUENCE_NUMBER')
        
    def __init__(self, last_event_id=EventId(), iterator='TRIM_HORIZON'):
        
        assert (last_event_id is not None), 'last_event_event cannot be None'
        assert (iterator and iterator in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'TRIM_HORIZON', 'LATEST']), "iterator must be one of 'AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'TRIM_HORIZON', 'LATEST'."
        
        self.last_event_id = last_event_id
        self.iterator = iterator
        
    def to_dict(self):
        return { 'iteratorType': self.iterator, 'commitNum': self.last_event_id.commit_num, 'opNum': self.last_event_id.op_num }
      
class NeptuneStreamRdfRecord:

    def __init__(self, statement):
    
        self.json_response = statement
        self.statement = statement
        
        
    def to_dict(self):
        return {
            'stmt': self.statement
        }
        
    def json(self):
        return self.json_response
        
class NeptuneStreamPropertyGraphRecord:

    def __init__(self, json):
    
        self.json_response = json
        self.id = json['id']
        self.type = json['type']
        self.key = json['key']
        self.data_type = json['value']['dataType']
        self.value = json['value']['value']
        self.frm = json['from'] if 'from' in json else None
        self.to = json['to'] if 'to' in json else None
        
    def to_dict(self):
        d = {
            'id': self.id,
            'type': self.type,
            'key': self.key,
            'dataType': self.data_type,
            'value': self.value
        }
        
        if self.frm:
            d['from'] = self.frm
            
        if self.to:
            d['to'] = self.to
        
        return d
        
    def json(self):
        return self.json_response

class NeptuneStreamRecord:

    def __init__(self, json, stream_format):
    
        self.json_response = json
        self.commit_timestamp = json['commitTimestamp']
        self.event_id =  EventId(json['eventId']['commitNum'], json['eventId']['opNum'])
        self.op = json['op']
        self.is_last_op = json.get('isLastOp', 'false') == 'true'
        self.data = NeptuneStreamPropertyGraphRecord(json['data']) if stream_format in ['GREMLIN_JSON', 'PG_JSON'] else NeptuneStreamRdfRecord(json['data'])
        
    def to_dict(self):
        return {
            'commitTimestamp': self.commit_timestamp,
            'eventId': self.event_id.to_dict(),
            'op': self.op,
            'isLastOp': self.is_last_op,
            'data': self.data.to_dict()      
        }
        
    def json(self):
        return self.json_response
             

class NeptuneStreamResponse:

    def __init__(self, json):
    
        self.json_response = json
        self.last_event_id = EventId(json['lastEventId']['commitNum'], json['lastEventId']['opNum'])
        self.total_records = json['totalRecords']
        self.format = json['format']
        self.last_transaction_timestamp = json['lastTrxTimestamp']
        self.records_json = json['records']
        self.token = NeptuneStreamToken.after(self.last_event_id)
        
    def records(self):
        
        index = 0
        while index < self.total_records:
            yield NeptuneStreamRecord(self.records_json[index], self.format)
            index += 1
            
    def to_dict(self):
        return {
            'lastEventId': self.last_event_id.to_dict(),
            'records': self.records()
        }
        
    def json(self):
        return self.json_response

class NeptuneStream:
    
    def __init__(self, endpoint_type='propertygraph', endpoints=None):
    
        assert (endpoint_type and endpoint_type in ['sparql', 'pg', 'propertygraph', 'gremlin']), "endpoint_type must be one of 'sparql', 'pg', 'propertygraph', 'gremlin'."
             
        self.endpoint_type = endpoint_type
        
        if endpoints is None:
            self.endpoints = Endpoints()
        else:
            self.endpoints = endpoints
    
    def stream_endpoint(self):
    
        if self.endpoint_type in ['pg', 'propertygraph']:
            return self.endpoints.propertygraph_stream_endpoint()
        elif self.endpoint_type == 'sparql':
            return self.endpoints.sparql_stream_endpoint()
        elif self.endpoint_type == 'gremlin':
            return self.endpoints.gremlin_stream_endpoint()
            
    def earliest_event_id(self):
        response = self.poll(NeptuneStreamToken.trim_horizon(), limit=1)
        return response.last_event_id
        
    def latest_event_id(self):
        response = self.poll(NeptuneStreamToken.latest(), limit=1)
        return response.last_event_id
        
    def is_empty(self):
        try:
            self.latest_event_id()
            return False
        except:
            return True
            
    def all_records_for_commit(self, commit_num, batch_size=10):
    
        results = self.poll(NeptuneStreamToken.at(EventId(commit_num, 1)), limit=batch_size)
        allow_continue = True
        
        while allow_continue:
            for record in results.records():
                if record.event_id.commit_num == commit_num:
                    yield record
                else:
                    allow_continue = False
            if allow_continue:
                results = self.poll(token=results.token)
                
                
    def __poll_after(self, event_id, batch_size=10, wait=0):
        while True:
            try:
                return self.poll(NeptuneStreamToken.after(event_id), limit=batch_size)
            except:
                if wait <= 0:
                    return None
                else:
                    time.sleep(wait)
        
    def all_records_after(self, event_id, batch_size=10, wait=0):
        
        results = self.__poll_after(event_id, batch_size, wait)
        allow_continue = True
        
        while results:
            for record in results.records():
                yield record
            results = self.__poll_after(results.last_event_id, batch_size, wait)
                    
            
    def poll(self, token=None, limit=10):
    
        assert (limit > 0 and limit <= 100000), 'limit must be between 1 and 100000.'
    
        if not token:
            token = NeptuneStreamToken()
        
        params = token.to_dict()
        params['limit'] = limit
        
        request_parameters = self.stream_endpoint().prepare_request(querystring=params)
        
        response = requests.get(self.stream_endpoint().value(), params=params, headers=request_parameters.headers)  
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            raise Exception('{}: {}'.format(response.status_code, response.text))
            
        json_response = response.json()
        
        return NeptuneStreamResponse(json_response)