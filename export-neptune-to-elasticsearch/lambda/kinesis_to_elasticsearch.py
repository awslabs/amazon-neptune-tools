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

logger = logging.getLogger()
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
                logger.info('Elastic Search Index - amazon_neptune already exist')
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
    
    # Deaggregate all records in one call
    user_records = deaggregate_records(raw_kinesis_records)
    
    total_records = len(user_records)
    
    log_stream = {
            "records": [],
            "lastEventId": {
                "commitNum": -1,
                "opNum": 0
            },
            "totalRecords": total_records
        }
        
    for user_record in user_records:
        records_json = base64.b64decode(user_record['kinesis']['data'])
        try:          
            records = json.loads(records_json)
        except Exception as e:
            logger.error('Error parsing JSON: \'{}\': {}'.format(records_json, str(e)))
            raise e
        for record in records:
            log_stream['records'].append(record)
            log_stream['lastEventId']['commitNum'] = record['eventId']['commitNum']
            log_stream['lastEventId']['opNum'] = record['eventId']['opNum']
        
    
    logger.info('{} records to process'.format(total_records))    
    
    for result in handler.handle_records(log_stream):
        records_processed = result.records_processed
        logger.info('{} records processed'.format(records_processed))
        #metrics_publisher_client.publish_metrics(metrics_publisher_client.generate_record_processed_metrics(records_processed))
        
    logger.info('Finished bulk loading')
        


