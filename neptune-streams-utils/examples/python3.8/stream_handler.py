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
import lambda_function
from commons import *
from handler import AbstractHandler, HandlerResponse

from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints

logger = logging.getLogger('StreamHandler')
logger.setLevel(logging.INFO)

'''
This handler processes a batch of Neptune Stream events. 
If the event represents the creation of a vertex or edge, the handler queries Neptune for the details of the element.
The handler yields one HandlerResponse per batch of stream events.
'''
class StreamHandler(AbstractHandler):

    def handle_records(self, stream_log):
        
        params = json.loads(os.environ['AdditionalParams'])
        
        neptune_endpoint = params['neptune_cluster_endpoint']
        neptune_port = params['neptune_port']
        
        GremlinUtils.init_statics(globals())

        endpoints = Endpoints(neptune_endpoint=neptune_endpoint, neptune_port=neptune_port)
        gremlin_utils = GremlinUtils(endpoints)

        conn = gremlin_utils.remote_connection()
        g = gremlin_utils.traversal_source(connection=conn)

        records = stream_log[RECORDS_STR]
        
        last_op_num = None
        last_commit_num = None    
        count = 0
        
        try:
            for record in records:
                
                # Process record
                op = record[OPERATION_STR]
                data = record[DATA_STR]
                type = data['type']
                id = data['id']
                
                if op == ADD_OPERATION:
                    if type == 'vl':
                        logger.info(g.V(id).valueMap(True).toList())
                    if type == 'e':
                        logger.info(g.E(id).valueMap(True).toList())
                        
                # Update local checkpoint info
                last_op_num = record[EVENT_ID_STR][OP_NUM_STR]
                last_commit_num = record[EVENT_ID_STR][COMMIT_NUM_STR]
                count += 1
                   
            
        except Exception as e:
            logger.error('Error occurred - {}'.format(str(e)))
            raise e
        finally:
            try:
                conn.close()
                yield HandlerResponse(last_op_num, last_commit_num, count)     
            except Exception as e:
                logger.error('Error occurred - {}'.format(str(e)))
                raise e
            finally:
                conn.close()
            
       
        
            
        

