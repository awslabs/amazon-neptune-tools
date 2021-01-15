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

import json
import logging
import os
import boto3
import lambda_function
from commons import *
from handler import AbstractHandler, HandlerResponse

logger = logging.getLogger('StreamHandler')
logger.setLevel(logging.INFO)

class StreamHandler(AbstractHandler):

    def handle_records(self, stream_log):
        
        params = json.loads(os.environ['AdditionalParams'])
        delivery_stream_name = params['delivery_stream_name']
        
        client = boto3.client('firehose')

        records = stream_log[RECORDS_STR]
        
        last_op_num = None
        last_commit_num = None    
        count = 1
        
        firehose_records = []
        
        try:
            for record in records:
                
                # Process record
                if count % 500 == 0:
                    response = client.put_record_batch(
                        DeliveryStreamName=delivery_stream_name,
                        Records= firehose_records
                    )
                    logger.info(response)
                    logger.info(len(firehose_records))
                    firehose_records.clear()
                firehose_record = {
                    "Data": '{}\n'.format(json.dumps(record))
                }
                firehose_records.append(firehose_record)
                        
                # Update local checkpoint info
                last_op_num = record[EVENT_ID_STR][OP_NUM_STR]
                last_commit_num = record[EVENT_ID_STR][COMMIT_NUM_STR]
                count += 1
                
            if len(firehose_records) > 0:
                logger.info(len(firehose_records))
                response = client.put_record_batch(
                    DeliveryStreamName=delivery_stream_name,
                    Records=firehose_records
                )
                logger.info(response)
                   
            
        except Exception as e:
            logger.error('Error occurred - {}'.format(str(e)))
            raise e
        finally:
            try:
                yield HandlerResponse(last_op_num, last_commit_num, count)     
            except Exception as e:
                logger.error('Error occurred - {}'.format(str(e)))
                raise e

            
       
        
            
        

