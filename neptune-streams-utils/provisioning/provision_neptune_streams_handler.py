#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

import boto3
import fire
import uuid
import json
from tabulate import tabulate

def provison_neptune_streams_handler(
    cluster_id, 
    handler_s3_bucket,
    handler_s3_key,
    handler_name=None,
    additional_params={},
    query_engine='Gremlin', 
    region='us-east-1', 
    lambda_memory_size_mb=512,
    lambda_runtime='python3.6',
    lambda_logging_level='INFO',
    managed_policy_arns=[],
    batch_size=100,
    max_polling_wait_time_seconds=60,
    max_polling_interval_seconds=600,
    step_function_fallback_period=1,
    step_function_fallback_period_unit='minute',
    notification_email='',
    create_cloudwatch_alarm=False,
    application_name=None, 
    dry_run=False):
    
    if lambda_runtime not in ['python3.6', 'java8', 'python', 'java']:
        raise Exception('lambda_runtime must be python, java, python3.6 or java8: ' + lambda_runtime)
        
    if lambda_logging_level not in ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']:
        raise Exception('lambda_logging_level must be DEBUG, INFO, WARN, ERROR or FATAL: ' + lambda_logging_level)
        
    if step_function_fallback_period_unit not in ['minutes', 'minute', 'hours', 'hour', 'days', 'day']:
        raise Exception('step_function_fallback_period_unit must be minutes, minute, hours, hour, days or day: ' + step_function_fallback_period_unit)
        
    if step_function_fallback_period == 1 and step_function_fallback_period_unit not in ['minute', 'hour', 'day']:
        raise Exception('step_function_fallback_period_unit must be singular if step_function_fallback_period = 1: ' + step_function_fallback_period_unit)
        
    if create_cloudwatch_alarm and not notification_email:
        raise('You must supply a notification_email if creating a CloudWatch alarm')
    
    neptune_query_engine = query_engine.lower().capitalize()
    
    if neptune_query_engine not in ['Sparql', 'Gremlin']:
        raise Exception('query_engine must be Sparql or Gremlin: ' + neptune_query_engine)
        
    if lambda_runtime == 'python':
        lambda_runtime = 'python3.6'
        
    if lambda_runtime == 'java':
        lambda_runtime = 'java8'  
        
    if not handler_name:
        handler_name = 'stream_handler.StreamHandler'

    id_suffix = str(uuid.uuid4()).split('-')[0]
    if not application_name:
        application_name = 'stream-handler-{}'.format(id_suffix)
        
    managed_policies = ','.join(managed_policy_arns)
    
    neptune = boto3.client('neptune', region_name=region)
    ec2 = boto3.client('ec2', region_name=region)
    cfn = boto3.client('cloudformation', region_name=region)

    describe_db_clusters_response = neptune.describe_db_clusters(
        DBClusterIdentifier=cluster_id
    )

    db_cluster = describe_db_clusters_response.get('DBClusters')[0]

    cluster_endpoint = db_cluster.get('Endpoint')
    reader_endpoint = db_cluster.get('ReaderEndpoint')
    port = db_cluster.get('Port')
    stream_endpoint = 'https://{}:{}/{}/stream'.format(cluster_endpoint, port, query_engine.lower())
    is_iam_auth_enabled = str(db_cluster.get('IAMDatabaseAuthenticationEnabled')).lower()
    cluster_resource_id = db_cluster.get('DbClusterResourceId')

    security_group_ids = list(map(lambda s: s.get('VpcSecurityGroupId'), db_cluster.get('VpcSecurityGroups')))

    instance_id = db_cluster.get('DBClusterMembers')[0].get('DBInstanceIdentifier')

    describe_db_instances_response = neptune.describe_db_instances(
        DBInstanceIdentifier=instance_id
    )

    subnet_group = describe_db_instances_response.get('DBInstances')[0].get('DBSubnetGroup')

    vpc_id = subnet_group.get('VpcId')

    subnet_ids = list(map(lambda s: s.get('SubnetIdentifier'), subnet_group.get('Subnets')))

    describe_subnets_response = ec2.describe_subnets(SubnetIds=subnet_ids)

    describe_route_tables_response = ec2.describe_route_tables(
        Filters=[
            {
                'Name': 'association.subnet-id',
                'Values': subnet_ids
            },
        ]
    )

    route_tables = describe_route_tables_response.get('RouteTables')

    if len(route_tables) == 0:
        raise Exception('You must explictly associate your subnets ' + str(subnet_ids) + ' to a Route Table')

    route_table_ids = list(set(map(lambda s: s.get('RouteTableId'), route_tables)))

    describe_vpc_endpoints_response = ec2.describe_vpc_endpoints(
        Filters=[
            {
                'Name': 'vpc-id',
                'Values': [
                    vpc_id
                ]
            },
        ]
    )

    vpc_endpoint_services = list(map(lambda s: s.get('ServiceName'), describe_vpc_endpoints_response.get('VpcEndpoints')))

    create_ddb_endpoint = str(not(any(e.endswith('.dynamodb') for e in vpc_endpoint_services))).lower()
    create_monitoring_endpoint = str(not(any(e.endswith('.monitoring') for e in vpc_endpoint_services))).lower()

    describe_vpc_attribute_response = ec2.describe_vpc_attribute(
        Attribute='enableDnsSupport',
        VpcId=vpc_id
    )

    if not describe_vpc_attribute_response.get('EnableDnsSupport').get('Value'):
        raise Exception('You must enable DNS resolution in your VPC in order to use the DynamoDB Gateway Endpoint')
        
    default_additional_params = {
        'cluster_id': cluster_id,
        'neptune_cluster_endpoint': cluster_endpoint,
        'neptune_reader_endpoint': reader_endpoint,
        'neptune_port': port,
        'iam_auth_enabled': is_iam_auth_enabled == 'true',
        'NeptuneCluster': '{}:{}'.format(cluster_endpoint, port)
    }
    
    for (k,v) in additional_params.items():
        default_additional_params[k] = v
        
    additional_params_json = json.dumps(default_additional_params)

    print('APPLICATION: {}'.format(application_name))
    
    print()
    print('NEPTUNE')
    print(tabulate([
        ['Parameter', 'Value', 'Description'],
        ['stream_endpoint', stream_endpoint, 'Endpoint for source Neptune Stream. This is of the form http(s)://<cluster>:<port>/gremlin/stream or http(s)://<cluster>:<port>/sparql/stream.'],
        ['is_iam_auth_enabled', is_iam_auth_enabled, 'Flag to determine if IAM Auth is enabled for source Neptune cluster.']
    ],headers="firstrow",tablefmt="fancy_grid"))
    
    print()
    print('HANDLER')
    print(tabulate([
        ['Parameter', 'Value', 'Description'],
        ['handler_s3_bucket', handler_s3_bucket, 'Handler Lambda S3 bucket.'],
        ['handler_s3_key', handler_s3_key, 'Handler Lambda S3 key.'],
        ['handler_name', handler_name, 'Name of the handler for processing stream records.'],
        ['additional_params', additional_params_json, 'Additional params to be supplied to the handler via an AdditionalParams environment variable in the form of a JSON object.']
    ],headers="firstrow",tablefmt="fancy_grid"))
    
    print()
    print('POLLING FRAMEWORK LAMBDA FUNCTION')
    print(tabulate([
        ['Parameter', 'Value', 'Description'],
        ['lambda_memory_size_mb', lambda_memory_size_mb, 'Poller Lambda memory size (in MB).'],
        ['lambda_runtime', lambda_runtime, 'Poller Lambda runtime (python3.6 or java8).'],
        ['lambda_logging_level', lambda_logging_level, 'Poller Lambda logging level.'],
        ['managed_policies', managed_policies, 'Comma-delimited list of ARNs of managed policies to be attached to Lambda execution role (for accessing other AWS resources from your handler).']
    ],headers="firstrow",tablefmt="fancy_grid"))
    
    print()
    print('POLLING FRAMEWORK CONFIG')
    print(tabulate([
        ['Parameter', 'Value', 'Description'],
        ['batch_size', batch_size, 'Number of records to be read from stream in each batch. Should be between 1 to 10000.'],
        ['max_polling_wait_time_seconds', max_polling_wait_time_seconds, 'Maximum wait time in seconds between two successive polling from stream. Set value to 0 sec for continuous polling. Maximum value can be 3600 sec (1 hour).'],
        ['max_polling_interval_seconds', max_polling_interval_seconds, 'Period for which we can continuously poll stream for records on one Lambda instance. Should be between 5 sec to 900 sec. This parameter is used to set Poller Lambda Timeout.'],
        ['step_function_fallback_period', step_function_fallback_period, 'Period after which Step Function is invoked using CloudWatch Events to recover from failure.'],
        ['step_function_fallback_period_unit', step_function_fallback_period_unit, 'Should be one of minutes, minute, hours, hour, days, day.'],
        ['create_cloudwatch_alarm', str(create_cloudwatch_alarm).lower(), 'Flag used to determine whether to create CloudWatch alarm.'],
        ['notification_email', notification_email, 'Email address for CloudWatch alarm notification.']
    ],headers="firstrow",tablefmt="fancy_grid"))
    
    print()
    print('NETWORK')
    print(tabulate([
        ['Parameter', 'Value', 'Description'],
        ['vpc_id', vpc_id, 'VPC of your Neptune database.'],
        ['subnet_ids', subnet_ids, 'Neptune subnets.'],
        ['security_group_ids', security_group_ids, 'VPC security groups.'],
        ['route_table_ids', route_table_ids, 'Comma-delimited list of route table ids associated with the Neptune subnets.'],
        ['create_ddb_endpoint', create_ddb_endpoint, 'Flag used to determine whether to create DynamoDB VPC endpoint.'],
        ['create_monitoring_endpoint', create_monitoring_endpoint, 'Flag used to determine whether to create Monitoring VPC endpoint.']
    ],headers="firstrow",tablefmt="fancy_grid"))
    
    print()
    

    params = [
        {
            'ParameterKey': 'ApplicationName',
            'ParameterValue': application_name
        },
        {
            'ParameterKey': 'AdditionalParams',
            'ParameterValue': additional_params_json
        },
        {
            'ParameterKey': 'LambdaS3Bucket',
            'ParameterValue': handler_s3_bucket
        },
        {
            'ParameterKey': 'LambdaS3Key',
            'ParameterValue': handler_s3_key
        },
        {
            'ParameterKey': 'LambdaMemorySize',
            'ParameterValue': str(lambda_memory_size_mb)
        },
        {
            'ParameterKey': 'LambdaRuntime',
            'ParameterValue': lambda_runtime
        },
        {
            'ParameterKey': 'LambdaLoggingLevel',
            'ParameterValue': lambda_logging_level
        },
        {
            'ParameterKey': 'ManagedPolicies',
            'ParameterValue': managed_policies
        },
        {
            'ParameterKey': 'StreamRecordsHandler',
            'ParameterValue': handler_name
        },
        {
            'ParameterKey': 'StreamRecordsBatchSize',
            'ParameterValue': str(batch_size)
        },
        {
            'ParameterKey': 'MaxPollingWaitTime',
            'ParameterValue': str(max_polling_wait_time_seconds)
        },
        {
            'ParameterKey': 'MaxPollingInterval',
            'ParameterValue': str(max_polling_interval_seconds)
        },
        
        {
            'ParameterKey': 'NeptuneStreamEndpoint',
            'ParameterValue': stream_endpoint
        },
        {
            'ParameterKey': 'IAMAuthEnabledOnSourceStream',
            'ParameterValue': is_iam_auth_enabled
        },
        {
            'ParameterKey': 'StreamDBClusterResourceId',
            'ParameterValue': cluster_resource_id
        },
        {
            'ParameterKey': 'StepFunctionFallbackPeriod',
            'ParameterValue': str(step_function_fallback_period)
        },
        {
            'ParameterKey': 'StepFunctionFallbackPeriodUnit',
            'ParameterValue': step_function_fallback_period_unit
        },
        {
            'ParameterKey': 'NotificationEmail',
            'ParameterValue': notification_email
        },
        {
            'ParameterKey': 'VPC',
            'ParameterValue': vpc_id
        },
        {
            'ParameterKey': 'SubnetIds',
            'ParameterValue': ",".join(subnet_ids)
        },
        {
            'ParameterKey': 'SecurityGroupIds',
            'ParameterValue': ",".join(security_group_ids)
        },
        {
            'ParameterKey': 'RouteTableIds',
            'ParameterValue': ",".join(route_table_ids)
        },
        {
            'ParameterKey': 'CreateDDBVPCEndPoint',
            'ParameterValue': create_ddb_endpoint
        },
        {
            'ParameterKey': 'CreateMonitoringEndPoint',
            'ParameterValue': create_monitoring_endpoint
        },
        {
            'ParameterKey': 'CreateCloudWatchAlarm',
            'ParameterValue': str(create_cloudwatch_alarm).lower()
        }    
    ]

    if not dry_run:
        create_stack_response = cfn.create_stack(
            StackName='streams-{}'.format(id_suffix),
            TemplateURL='https://s3.amazonaws.com/aws-neptune-customer-samples/neptune-stream/neptune_stream_poller_nested_full_stack.json',
            Parameters=params,
            DisableRollback=True,
            TimeoutInMinutes=30,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )

        return create_stack_response
    else:
        return 'Dry run: {}'.format(params)

if __name__ == '__main__':
  fire.Fire(provison_neptune_streams_handler)
