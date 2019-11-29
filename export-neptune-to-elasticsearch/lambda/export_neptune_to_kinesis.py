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

import json
import os
import boto3
import logging
from datetime import datetime

client = boto3.client('batch')

logger = logging.getLogger()

def trigger_neptune_export():

    neptune_export_jar_uri = os.environ['NEPTUNE_EXPORT_JAR_URI']
    neptune_endpoint = os.environ['NEPTUNE_ENDPOINT']
    neptune_port = os.environ['NEPTUNE_PORT']
    neptune_engine = os.environ['NEPTUNE_ENGINE']
    stream_name = os.environ['STREAM_NAME']
    region = os.environ['AWS_REGION']
    use_iam_auth = '' if neptune_engine == 'sparql' else ' --use-iam-auth'
    
    export_command = 'export-pg' if neptune_engine == 'gremlin' else 'export-rdf'
    
    command = 'wget {} && export SERVICE_REGION="{}" && java -jar neptune-export.jar {} -e {} -p {} -d results --output stream --stream-name {} --region {} --format neptuneStreamsJson --log-level info --use-ssl{}'.format(
        neptune_export_jar_uri, 
        region,
        export_command, 
        neptune_endpoint, 
        neptune_port,
        stream_name, 
        region,
        use_iam_auth)
        
    logger.info('Command: {}'.format(command))
    
    submit_job_response = client.submit_job(
        jobName='export-neptune-to-kinesis-{}'.format(round(datetime.utcnow().timestamp() * 1000)),
        jobQueue='export-neptune-to-kinesis-queue',
        jobDefinition='export-neptune-to-kinesis-job',
        containerOverrides={
            'command': [
                'sh',
                '-c',
                command
            ]
        }
    )
    
    return submit_job_response

def lambda_handler(event, context):
    
    result = trigger_neptune_export()
    
    job_name = result['jobName']
    job_id = result['jobId']
    
    return {
            'jobName': job_name,
            'jobId': job_id
        }
