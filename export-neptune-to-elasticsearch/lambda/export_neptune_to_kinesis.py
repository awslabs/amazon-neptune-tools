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
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

client = boto3.client('batch')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def pre_trigger():
    # clears `amazon_neptune` indices before we run rest of the job
    print("Started PreTrigger Job")
    host = os.environ['ELASTIC_SEARCH_ENDPOINT'] # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com
    region = os.environ['AWS_REGION']
    service = 'es'
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region, service)

    client = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = auth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection,
        pool_maxsize = 20
    )

    logger.info("Listing Indices Now")
    response = client.indices.get_alias("*")
    logger.info(response)

    logger.info("Creating Index Now")
    index_name = "amazon_neptune"
    index_body = {
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 2,
        },
    }
    response = client.indices.create(index=index_name, body=index_body)
    logger.info(response)
    
    logger.info("Listing Indices Now")
    response = client.indices.get_alias("*")
    logger.info(response)
    
    logger.info("Deleting Index Now")    
    response = client.indices.delete(index="your_index_name")
    logger.info(response)
    
    logger.info("Listing Indices Now")
    response = client.indices.get_alias("*")
    logger.info(response)

def trigger_neptune_export():

    pre_trigger()
    neptune_export_jar_uri = os.environ['NEPTUNE_EXPORT_JAR_URI']
    neptune_endpoint = os.environ['NEPTUNE_ENDPOINT']
    neptune_port = os.environ['NEPTUNE_PORT']
    neptune_engine = os.environ['NEPTUNE_ENGINE']
    stream_name = os.environ['STREAM_NAME']
    job_suffix = os.environ['JOB_SUFFIX']
    region = os.environ['AWS_REGION']
    concurrency = os.environ['CONCURRENCY']
    scope = os.environ['EXPORT_SCOPE']
    additional_params = os.environ['ADDITIONAL_PARAMS']
    clone_cluster = os.environ.get('CLONE_CLUSTER')

    if additional_params:
        additional_params = additional_params if additional_params.startswith(' ') else ' {}'.format(additional_params)
    else:
        additional_params = ''
        
    use_iam_auth = '' if neptune_engine == 'sparql' else ' --use-iam-auth' 
    export_command = 'export-pg' if neptune_engine == 'gremlin' else 'export-rdf'
    concurrency_param = ' --concurrency {}'.format(concurrency) if neptune_engine == 'gremlin' else ''
    scope_param = ' --scope {}'.format(scope) if neptune_engine == 'gremlin' else ''
    clone_cluster_param = ' --clone-cluster' if clone_cluster and clone_cluster.lower() == 'true' else ''
            
    command = 'df -h && rm -rf neptune-export.jar && wget {} -nv && export SERVICE_REGION="{}" && java -Xms16g -Xmx16g -jar neptune-export.jar {} -e {} -p {} -d /neptune/results --output stream --stream-name {} --region {} --format neptuneStreamsJson --use-ssl{}{}{}{}{}'.format(
        neptune_export_jar_uri, 
        region,
        export_command, 
        neptune_endpoint, 
        neptune_port,
        stream_name, 
        region,
        use_iam_auth,
        concurrency_param,
        scope_param,
        clone_cluster_param,
        additional_params)
        
    logger.info('Command: {}'.format(command))
    
    submit_job_response = client.submit_job(
        jobName='export-neptune-to-kinesis-{}-{}'.format(job_suffix, round(datetime.utcnow().timestamp() * 1000)),
        jobQueue='export-neptune-to-kinesis-queue-{}'.format(job_suffix),
        jobDefinition='export-neptune-to-kinesis-job-{}'.format(job_suffix),
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
