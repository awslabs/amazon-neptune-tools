#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#  
#      http://www.apache.org/licenses/LICENSE-2.0
#  
#  or in the "license" file accompanying this file. This file is distributed 
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
#  express or implied. See the License for the specific language governing 
#  permissions and limitations under the License.

from aws_kinesis_agg.deaggregator import deaggregate_records, iter_deaggregate_records
#from metrics_publisher import MetricsPublisher
import importlib
import logging
import base64
import six
import os
import json
import neptune_to_es

logger = logging.getLogger()
logger.setLevel(logging.INFO)

neptune_engine = os.environ['NEPTUNE_ENGINE']
stream_name = os.environ['STREAM_NAME']
handler_name = 'neptune_to_es.neptune_sparql_es_handler.ElasticSearchSparqlHandler' if neptune_engine == 'sparql' else 'neptune_to_es.neptune_gremlin_es_handler.ElasticSearchGremlinHandler'

# Dummy values
os.environ["StreamRecordsBatchSize"] = "100"
os.environ["MaxPollingWaitTime"] = "1"
os.environ["Application"] = ""
os.environ["LeaseTable"] = ""
os.environ["LoggingLevel"] = "INFO"
os.environ["MaxPollingInterval"] = "1"
os.environ["NeptuneStreamEndpoint"] = ""
os.environ["StreamRecordsHandler"] = handler_name

#metrics_publisher_client = MetricsPublisher()

def get_handler_instance(handler_name, retry_count=0):

    """
    Get Handler instance given a handler name with module
    :param handler_name: the handler class name with module.
    :return: Handler instance

    """
    logger.info('Handler: {}'.format(handler_name))
    
    try:
        parts = handler_name.rsplit('.', 1)
        module = importlib.import_module(parts[0])
        cls = getattr(module, parts[1])
        return cls()
    except Exception as e:
        error_msg = str(e)
        if 'resource_already_exists_exception' in error_msg:
            if retry_count > 3:           
                logger.info('Elastic Search Index - amazon_neptune already exists')
                raise e;
            else:
                return get_handler_instance(handler_name, retry_count + 1)
        else:
            logger.error('Error occurred while creating handler instance for {} - {}.'.format(handler_name, error_msg))
            raise e

handler = get_handler_instance(handler_name)

def lambda_bulk_handler(event, context):
    """A Python AWS Lambda function to process Kinesis aggregated
    records in a bulk fashion."""
    
    logger.info('Starting bulk loading')
    
    raw_kinesis_records = event['Records']
    
    logger.info('Aggregated Kinesis record count: {}'.format(len(raw_kinesis_records)))
    
    # Deaggregate all records in one call
    user_records = deaggregate_records(raw_kinesis_records)
    
    total_records = len(user_records)
    
    logger.info('Deaggregated record count: {}'.format(total_records))
    
    log_stream = {
            "records": [],
            "lastEventId": {
                "commitNum": -1,
                "opNum": 0
            },
            "totalRecords": total_records
        }
        
    first_commit_num = None
    first_op_num = None
    prev_commit_num = None
    prev_op_num = None
        
    for user_record in user_records:
        records_json = base64.b64decode(user_record['kinesis']['data'])
        try:          
            records = json.loads(records_json)
        except Exception as e:
            logger.error('Error parsing JSON: \'{}\': {}'.format(records_json, str(e)))
            raise e
        for record in records:
            
            commit_num = record['eventId']['commitNum']
            op_num = record['eventId']['opNum']
            
            if not first_commit_num:
                first_commit_num = commit_num
                
            if not first_op_num:
                first_op_num = op_num
                
            #logger.info('Stream record: (commitNum: {}, opNum: {})'.format(commit_num, op_num)) 
            
            #if prev_commit_num and commit_num < prev_commit_num:
            #    logger.warn('Current commitNum [{}] is less than previous commitNum [{}]'.format(commit_num, prev_commit_num))
                
            if prev_commit_num and commit_num == prev_commit_num:
                if prev_op_num and op_num < prev_op_num:
                    logger.warn('Current opNum [{}] is less than previous opNum [{}] (commitNum [{}])'.format(op_num, prev_op_num, commit_num))
                    
            log_stream['records'].append(record)
            
            prev_commit_num = commit_num
            prev_op_num = op_num
            
    log_stream['lastEventId']['commitNum'] = prev_commit_num if prev_commit_num else -1
    log_stream['lastEventId']['opNum'] = prev_op_num if prev_op_num else 0
        
    logger.info('Log stream record count: {}'.format(len(log_stream['records']))) 
    logger.info('First record: (commitNum: {}, opNum: {})'.format(first_commit_num, first_op_num))
    logger.info('Last record: (commitNum: {}, opNum: {})'.format(prev_commit_num, prev_op_num))   
    
    for result in handler.handle_records(log_stream):
        records_processed = result.records_processed
        logger.info('{} records processed'.format(records_processed))
        #metrics_publisher_client.publish_metrics(metrics_publisher_client.generate_record_processed_metrics(records_processed))
        
    logger.info('Finished bulk loading')
        


