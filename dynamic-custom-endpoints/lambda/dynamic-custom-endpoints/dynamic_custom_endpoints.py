#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import boto3
import json
import time
import logging
import os
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
#logger.addHandler(logging.StreamHandler(sys.stdout))

instance_tags = {}
cache_instance_tags = False

neptune = boto3.client('neptune')

def get_writer(cluster_members):
    filtered = list(filter(lambda m: m['IsClusterWriter'], cluster_members))
    if filtered:
        return filtered[0]
    else:
        return None
        
def get_readers(cluster_members):
    return list(filter(lambda m: m['IsClusterWriter'] == False, cluster_members))
    
def get_tags(arn):

    if cache_instance_tags:
        tags = instance_tags.get(arn, None)
        
        if tags:
            return tags
        
    list_tags_for_resource_response = neptune.list_tags_for_resource(ResourceName=arn)
    tags =  list_tags_for_resource_response['TagList']
    
    if cache_instance_tags:    
        instance_tags[arn] = tags
    
    return tags
    
def get_batch_instance_details(cluster_id):
    
    describe_db_instances_response = neptune.describe_db_instances(
        Filters=[
            {
                'Name': 'db-cluster-id',
                'Values': [
                    cluster_id,
                ]
            },
            {
                'Name': 'engine',
                'Values': [
                    'neptune',
                ]
            }
        ]
    )
    
    return describe_db_instances_response['DBInstances']
    
def get_per_instance_details(instance_ids, resource_prefix):

    results = []
    
    for instance_id in instance_ids:
    
        if instance_id.startswith(resource_prefix):
    
            describe_db_instances_response = neptune.describe_db_instances(
                DBInstanceIdentifier=instance_id
            )
            
            for i in describe_db_instances_response['DBInstances']:
                results.append(i)
            
    return results
        

def get_cluster_metadata(cluster_id, resource_prefix=None):

    describe_db_clusters_response = neptune.describe_db_clusters(
        DBClusterIdentifier=cluster_id,
        Filters=[
            {
                'Name': 'engine',
                'Values': [
                    'neptune',
                ]
            }
        ]
    )
    
    db_clusters = describe_db_clusters_response.get('DBClusters')
    
    if not db_clusters:
        raise Exception('Unable to find Neptune cluster \'{}\''.format(cluster_id))
    
    db_cluster = describe_db_clusters_response.get('DBClusters')[0]
    
    cluster_endpoint = db_cluster['Endpoint']
    reader_endpoint = db_cluster['ReaderEndpoint']
    
    cluster_members = db_cluster['DBClusterMembers']
    
    writer = get_writer(cluster_members)
    readers = get_readers(cluster_members)
    
    primary = writer['DBInstanceIdentifier'] if writer else None
    replicas = list(map(lambda m: m['DBInstanceIdentifier'], readers))
    
    instance_ids = [i['DBInstanceIdentifier'] for i in cluster_members]
    
    instance_details = None
    
    if resource_prefix:
        instance_details = get_per_instance_details(instance_ids, resource_prefix)
    else:
        instance_details = get_batch_instance_details(cluster_id)
    
    instances = []
    
    for i in instance_details:
        
        instance_id = i['DBInstanceIdentifier']
        
        role = 'unknown'
        if primary and primary == instance_id:
            role = 'writer'
        elif instance_id in replicas:
            role = 'reader'
            
        endpoint = i.get('Endpoint', None)
        address = endpoint['Address'] if endpoint else None
        
        status = i.get('DBInstanceStatus', None)
        availability_zone = i.get('AvailabilityZone', None)
        instance_type = i.get('DBInstanceClass', None)
        promotion_tier = i.get('PromotionTier', None)
        
        arn = i.get('DBInstanceArn', None)
        
        tags = get_tags(arn) if arn else []
        
        instances.append({
            'instanceId': instance_id,
            'role': role,
            'endpoint': address,
            'status': status,
            'endpointIsAvailable': status in ['available', 'backing-up', 'modifying', 'upgrading'],
            'availabilityZone': availability_zone,
            'instanceType': instance_type,
            'promotionTier': promotion_tier,
            'tags': tags
        })
        
    return {
        'clusterEndpoint': cluster_endpoint,
        'readerEndpoint': reader_endpoint,
        'instances': instances
    }
    
def get_endpoint_status(cluster_id, endpoint_id):

    describe_db_cluster_endpoints_response = neptune.describe_db_cluster_endpoints(
        DBClusterIdentifier=cluster_id,
        DBClusterEndpointIdentifier=endpoint_id
    )
    
    endpoints = describe_db_cluster_endpoints_response['DBClusterEndpoints']
    
    exists = len(endpoints) > 0
    status = endpoints[0]['Status'] if exists else None
    instance_ids = endpoints[0]['StaticMembers'] if exists else None
    
    return (exists, status, instance_ids)

def create_custom_endpoint(cluster_id, endpoint_id, member_instance_ids, excluded_ids):

    logger.info('Creating custom endpoint...')
    logger.info('endpoint_id: {}'.format(endpoint_id))
    logger.info('member_instance_ids: {}'.format(member_instance_ids))
    logger.info('excluded_ids: {}'.format(excluded_ids))
    
    create_db_cluster_endpoint_response = None
    
    if member_instance_ids:
        create_db_cluster_endpoint_response = neptune.create_db_cluster_endpoint(
            DBClusterIdentifier=cluster_id,
            DBClusterEndpointIdentifier=endpoint_id,
            EndpointType='ANY',
            StaticMembers=member_instance_ids,
            Tags=[
                    {
                        'Key': 'awslabs:owner',
                        'Value': 'dynamic-custom-endpoints'
                    }
                ]
        )
    else:
        create_db_cluster_endpoint_response = neptune.create_db_cluster_endpoint(
            DBClusterIdentifier=cluster_id,
            DBClusterEndpointIdentifier=endpoint_id,
            EndpointType='ANY',
            ExcludedMembers=excluded_ids,
            Tags=[
                    {
                        'Key': 'awslabs:owner',
                        'Value': 'dynamic-custom-endpoints'
                    }
                ]
        )
    
    return create_db_cluster_endpoint_response['Endpoint']

def update_custom_endpoint(endpoint_id, member_instance_ids, excluded_ids):

    logger.info('Updating custom endpoint...')
    logger.info('endpoint_id: {}'.format(endpoint_id))
    logger.info('member_instance_ids: {}'.format(member_instance_ids))
    logger.info('excluded_ids: {}'.format(excluded_ids))

    modify_db_cluster_endpoint_response = None
    
    if member_instance_ids:
        modify_db_cluster_endpoint_response = neptune.modify_db_cluster_endpoint(
            DBClusterEndpointIdentifier=endpoint_id,
            EndpointType='ANY',
            StaticMembers=member_instance_ids
        )
    else:
        modify_db_cluster_endpoint_response = neptune.modify_db_cluster_endpoint(
            DBClusterEndpointIdentifier=endpoint_id,
            EndpointType='ANY',
            ExcludedMembers=excluded_ids
        )

    return modify_db_cluster_endpoint_response['Endpoint']
    
def custom_endpoint_requires_update(existing_member_ids, new_member_ids):
    existing_member_ids.sort()
    new_member_ids.sort()
    return existing_member_ids != new_member_ids

def configure_custom_endpoint(cluster_id, endpoint_id, member_instance_ids, excluded_ids):

    (exists, status, instance_ids) = get_endpoint_status(cluster_id, endpoint_id)
    
    if exists:
    
        if status in ['deleting', 'inactive']:
            logger.info('Unable to configure endpoint \'{}\' because it is {}'.format(endpoint_id, status))
            return
        
        allow_continue = status == 'available'
        while not allow_continue:
            logger.info('Sleeping for 5 seconds because endpoint \'{}\' is {}'.format(endpoint_id, status))
            time.sleep(5)
            (exists, status, instance_ids) = get_endpoint_status(cluster_id, endpoint_id)
            allow_continue = status == 'available'
            
        requires_update = custom_endpoint_requires_update(instance_ids, member_instance_ids)
        
        if requires_update:               
            update_custom_endpoint(endpoint_id, member_instance_ids, excluded_ids)
            logger.info('Endpoint \'{}\' updated'.format(endpoint_id))
        else:
            logger.info('Endpoint \'{}\' is already up-to-date'.format(endpoint_id))
    else:
        create_custom_endpoint(cluster_id, endpoint_id, member_instance_ids, excluded_ids)
        logger.info('Endpoint \'{}\' created'.format(endpoint_id))
        
import abc
import six

@six.add_metaclass(abc.ABCMeta)
class Comparator:

    @abc.abstractmethod   
    def are_equal(self, x, y):
        pass
        
    @abc.abstractmethod   
    def are_not_equal(self, x, y):
        pass
 
    @abc.abstractmethod   
    def greater_than(self, x, y):
        pass
        
    @abc.abstractmethod   
    def less_than(self, x, y):
        pass

    @abc.abstractmethod   
    def as_string(self, op_name):
        pass
        
class DefaultComparator(Comparator):
     
    def are_equal(self, x, y):
        return x == y
        
    def are_not_equal(self, x, y):
        return x != y
        
    def greater_than(self, x, y):
        return x > y
         
    def less_than(self, x, y):
        return x < y
  
    def as_string(self, op_name):
        if op_name == 'are_equal':
            return '=='
        elif op_name == 'are_not_equal':
            return '!='
        elif op_name == 'greater_than':
            return '>'
        elif op_name == 'less_than':
            return '<'
        else:
            raise Exception('Unrecognized op_name: {}'.format(op_name))
        
class TagComparator(Comparator):

    def are_equal(self, x, y):
        if not x or not y:
            return False
        return x['Key'] == y['Key'] and x['Value'] == y['Value']
        
    def are_not_equal(self, x, y):
        return x['Key'] != y['Key'] or x['Value'] != y['Value']
        
    def greater_than(self, x, y):
        raise Exception('Unsupported operation: greater_than')
         
    def less_than(self, x, y):
        raise Exception('Unsupported operation: less_than')
 
    def as_string(self, op_name):
        if op_name == 'are_equal':
            return 'equals'
        elif op_name == 'are_not_equal':
            return 'not equal'
        else:
            raise Exception('Unrecognized op_name: {}'.format(op_name))

@six.add_metaclass(abc.ABCMeta)
class Operator:

    @abc.abstractmethod   
    def apply(self, o):
        pass
        
    @abc.abstractmethod   
    def as_string(self):
        pass      

@six.add_metaclass(abc.ABCMeta)
class LogicalOperator(Operator):

    @abc.abstractmethod
    def __init__(self, conditions):
        Operator.__init__(self)
        if type(conditions) is not list:
            raise Exception('Expected list, but received: {}'.format(conditions))
        self.conditions = conditions
        
@six.add_metaclass(abc.ABCMeta)
class ComparisonOperator(Operator):

    @abc.abstractmethod
    def __init__(self, expected, comparator):
        Operator.__init__(self)
        self.expected = expected
        self.comparator = comparator
        
class AndOperator(LogicalOperator):

    def __init__(self, conditions):
        LogicalOperator.__init__(self, conditions)
        
    def apply(self, o):
        result = True
        for condition in self.conditions:
            result &= condition.apply(o)
        return result
        
    def as_string(self):
        return '[{}]'.format('] AND ['.join(map(lambda x:x.as_string(),self.conditions)))
        
class OrOperator(LogicalOperator):

    def __init__(self, conditions):
        LogicalOperator.__init__(self, conditions)
        
    def apply(self, o):
        
        for condition in self.conditions:
            if condition.apply(o):
                return True
        return False
        
    def as_string(self):
        return '[{}]'.format('] OR ['.join(map(lambda x:x.as_string(),self.conditions))) 
        
class EqOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):   
        return self.comparator.are_equal(o, self.expected)
        
    def as_string(self):
        return '{} \'{}\''.format(self.comparator.as_string('are_equal'), self.expected)
        
class NotEqOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):   
        return self.comparator.are_not_equal(o, self.expected)
        
    def as_string(self):
        return '{} \'{}\''.format(self.comparator.as_string('are_not_equal'), self.expected)
        
class GreaterThanOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):   
        return self.comparator.greater_than(o, self.expected)
        
    def as_string(self):
        return '{} \'{}\''.format(self.comparator.as_string('greater_than'), self.expected)
        
class LessThanOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):   
        return self.comparator.less_than(o, self.expected)
        
    def as_string(self):
        return '{} \'{}\''.format(self.comparator.as_string('less_than'), self.expected)
        
class StartsWithOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o): 
        return o.startswith(self.expected)  
        
    def as_string(self):
        return 'starts with \'{}\''.format(self.expected)
        
class AllOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        expected = expected if type(expected) is list else [expected]
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):
        o = o if type(o) is list else [o]
        result = True
        for x in self.expected:
            found = False
            for y in o:
                if issubclass(type(x), ComparisonOperator):
                    if x.apply(y):
                        found = True
                elif self.comparator.are_equal(x, y):
                    found = True          
            result &= found
        return result
        
    def as_string(self):
        return 'contains all of {}'.format(self.expected)
        
class AnyOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        expected = expected if type(expected) is list else [expected]
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):
        o = o if type(o) is list else [o]
        for x in self.expected:
            for y in o:
                if issubclass(type(x), ComparisonOperator):
                    if x.apply(y):
                        return True
                elif self.comparator.are_equal(x, y):
                    return True
        return False
        
    def as_string(self):
        return 'contains any of {}'.format(self.expected)
        
class NoneOperator(ComparisonOperator):

    def __init__(self, expected, comparator=None):
        expected = expected if type(expected) is list else [expected]
        ComparisonOperator.__init__(self, expected, comparator)
        
    def apply(self, o):
        o = o if type(o) is list else [o]
        for x in self.expected:
            for y in o:
                if issubclass(type(x), ComparisonOperator):
                    if x.apply(y):
                        return False
                elif self.comparator.are_equal(x, y):
                    return False
        return True
        
    def as_string(self):
        return 'contains none of {}'.format(self.expected)
        
class TagOperator(ComparisonOperator):
    
    def __init__(self, key, value_operator, comparator=None):
        self.key = key
        self.value_operator = value_operator
        ComparisonOperator.__init__(self, None, None)
        
    def apply(self, o):
        if not is_tag(o):
            raise Exception('Expected tag, but received: {}'.format(o))
        k = o['Key']
        value = o['Value']
        return k == self.key and self.value_operator.apply(value)
        
    def as_string(self):
        return '{{tag with Key == \'{}\' and Value {}}}'.format(self.key, self.value_operator.as_string())
        
    def __repr__(self):
        return self.as_string()
       
        
class Matcher(Operator):

    def __init__(self, field_name, operator):
        Operator.__init__(self)
        self.field_name = field_name
        self.operator = operator
        
    def apply(self, o):    
        value = o[self.field_name]
        if not value:
            return False
        return self.operator.apply(value)   
    
    def as_string(self):
        return 'field \'{}\' {}'.format(self.field_name, self.operator.as_string())

def missing_operator_wrapper(comparator=DefaultComparator()):
    def wrapper(x):
        if type(x) is list:
            return AnyOperator(x, comparator)
        else:
            return EqOperator(x, comparator)
    return wrapper
    
def missing_tags_operator_wrapper(comparator=DefaultComparator()):
    def wrapper(x):
        if type(x) is list:
            return AllOperator(x, comparator)
        else:
            return EqOperator(x, comparator)
    return wrapper
    
    
def default_wrapper(x):
    if type(x) is list:
        if len(x) != 1:
            return x
        else:
            return x[0]
    else:
        return x
        
def is_tag(x):
    return len(x) == 2 and 'Key' in x and 'Value' in x
    
def parse_tag(tag):
    value = tag['Value']
    if type(value) is dict:
        key = tag['Key']
        value_operator = parse(value)
        return TagOperator(key, value_operator)
    else:
        return tag
    
def parse(config, wrapper=default_wrapper, comparator=None):
        
    if type(config) is list:
    
        conditions = []      
        for o in config:
            conditions.append(parse(o, comparator=comparator))
        if not wrapper:
            raise Exception('Expected wrapper function but received: None')
        
        return wrapper(conditions)
        
    elif type(config) is dict:
    
        keys = config.keys()
        
        if len(keys) == 0:
            raise Exception('Received empty dictionary')
        
        if is_tag(keys):
            if not wrapper:
                raise Exception('Expected wrapper function but received: None')
            return wrapper(parse_tag(config))
        
        first_key = list(keys)[0]
        
        if len(keys) == 1 and first_key in ['and', 'or', 'equals', 'eq', 'notEquals', 'notEq', 'gt', 'greaterThan', 'lt', 'lessThan', 'all', 'any', 'none', 'startsWith']:
            args = parse(config[first_key], comparator=comparator)
            if first_key == 'and':
                return AndOperator(args)
            elif first_key == 'or':
                return OrOperator(args)
            elif first_key in ['eq', 'equals']:
                return EqOperator(args, comparator=comparator)
            elif first_key in ['notEq', 'notEquals']:
                return NotEqOperator(args, comparator=comparator)
            elif first_key in ['gt', 'greaterThan']:
                return GreaterThanOperator(args, comparator=comparator)
            elif first_key in ['lt', 'lessThan']:
                return LessThanOperator(args, comparator=comparator)
            elif first_key == 'startsWith':
                return StartsWithOperator(args, comparator=comparator)
            elif first_key == 'all':
                return AllOperator(args, comparator=comparator)
            elif first_key == 'any':
                return AnyOperator(args, comparator=comparator)
            elif first_key == 'none':
                return NoneOperator(args, comparator=comparator)
        
        conditions = []
        
        for key in keys:
            if key in ['instanceId', 'instanceType', 'role', 'status', 'endpointIsAvailable', 'endpoint', 'availabilityZone', 'promotionTier']:
                operator = parse(config[key], wrapper=missing_operator_wrapper(), comparator=DefaultComparator())
                conditions.append(Matcher(key, operator))
            elif key == 'tags':
                operator = parse(config[key], wrapper=missing_tags_operator_wrapper(TagComparator()), comparator=TagComparator())
                conditions.append(Matcher(key, operator))
            else:
                raise Exception('Unrecognized key: {}'.format(key))
        
        return AndOperator(conditions)
                
    else:
        if not wrapper:
            raise Exception('Expected wrapper function but received: None')
        return wrapper(config)
        
        
def parse_config(config):
    conditions = parse(config)
    if type(conditions) is list:
        return AndOperator(conditions)
    else:
        return conditions
        
def get_instance_ids_for_specification(specification, instances):

    instance_ids = []
    excluded_ids = []
    
    conditions = parse_config(specification)
    
    logger.info('conditions: {}'.format(conditions.as_string()))
    
    for instance in instances:
    
        include = conditions.apply(instance)
               
        if include:
            instance_ids.append(instance['instanceId'])
        else:
            excluded_ids.append(instance['instanceId'])
            
    return (instance_ids, excluded_ids)
    
def apply_config(cluster_id, config, cluster_metadata):
    
    logger.info('Starting applying config...')
    
    if 'customEndpoints' not in config:
        logger.info('No customEndpoints present in config')
        return

    for custom_endpoint_config in config['customEndpoints']:
    
        endpoint_id = custom_endpoint_config['customEndpoint']
        specification = custom_endpoint_config['specification']
        
        logger.info('Starting applying config for endpoint \'{}\''.format(endpoint_id))
        logger.info('specification: {}'.format(specification))
        
        instances = cluster_metadata['instances']
       
        (instance_ids, excluded_ids) = get_instance_ids_for_specification(specification, instances)
               
        logger.info('instance_ids: {}'.format(instance_ids))
        logger.info('excluded_ids: {}'.format(excluded_ids))
           
        configure_custom_endpoint(cluster_id, endpoint_id, instance_ids, excluded_ids) 
        
        logger.info('Finished applying config for endpoint \'{}\''.format(endpoint_id))  
        
    logger.info('Finished applying config')     

def update_custom_endpoints_for_cluster(cluster_id, config, resource_prefix):

    cluster_metadata = get_cluster_metadata(cluster_id, resource_prefix)
        
    logger.info('cluster_metadata: {}'.format(cluster_metadata))
        
    apply_config(cluster_id, config, cluster_metadata) 
    
def lambda_handler(event, context):
    
    logger.info(event)
    
    cluster_id = os.environ['clusterId']   
    config_value = os.environ.get('config')
    resource_prefix = os.environ.get('resourcePrefix')
    
    logger.info('cluster_id: {}'.format(cluster_id))
    logger.info('resource_prefix: {}'.format(resource_prefix))
    
    if config_value:
        
        config = json.loads(config_value)
        logger.info('config: {}'.format(config))
        
        update_custom_endpoints_for_cluster(cluster_id, config, resource_prefix)
        
    else:
        logger.config('Config is empty')
    
    
import unittest

def get_instances_with_tags():
    return [
      {
        'instanceId': 'neptunedbinstance-1',
        'role': 'writer',
        'endpoint': 'neptunedbinstance-xxjzfxbbtplk.c5pfn0sfmklq.eu-west-2.neptune.amazonaws.com',
        'status': 'available',
        'endpointIsAvailable': True,
        'availabilityZone': 'eu-west-2b',
        'instanceType': 'db.r5.xlarge',
        'promotionTier': 1,
        'tags': [
          {
            'Key': 'Application',
            'Value': 'NeptuneCloudformation'
          },
          {
            'Key': 'Name',
            'Value': 'Neptune-test'
          },
          {
            'Key': 'Stack',
            'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'
          }
        ]
      },
      {
        'instanceId': 'neptunedbinstance-2',
        'role': 'reader',
        'endpoint': 'neptunedbinstance-xxjzfxbbtplk.c5pfn0sfmklq.eu-west-2.neptune.amazonaws.com',
        'status': 'available',
        'endpointIsAvailable': True,
        'availabilityZone': 'eu-west-2b',
        'instanceType': 'db.r5.xlarge',
        'promotionTier': 2,
        'tags': [
          {
            'Key': 'Application',
            'Value': 'NeptuneCloudformation'
          },
          {
            'Key': 'Name',
            'Value': 'Neptune-demo'
          },
          {
            'Key': 'Stack',
            'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'
          }
        ]
      }
    ]
    
def get_instances_without_tags():
    return [
      {
        'instanceId': 'neptunedbinstance-1',
        'role': 'writer',
        'endpoint': 'neptunedbinstance-xxjzfxbbtplk-1.c5pfn0sfmklq.eu-west-2.neptune.amazonaws.com',
        'status': 'available',
        'endpointIsAvailable': True,
        'availabilityZone': 'eu-west-2b',
        'instanceType': 'db.r5.xlarge',
        'promotionTier': 1,
        'tags': [
          
        ]
      },
      {
        'instanceId': 'neptunedbinstance-2',
        'role': 'reader',
        'endpoint': 'neptunedbinstance-xxjzfxbbtplk-2.c5pfn0sfmklq.eu-west-2.neptune.amazonaws.com',
        'status': 'available',
        'endpointIsAvailable': True,
        'availabilityZone': 'eu-west-2b',
        'instanceType': 'db.r5.xlarge',
        'promotionTier': 2,
        'tags': [
          
        ]
      },
      {
        'instanceId': 'neptunedbinstance-3',
        'role': 'reader',
        'endpoint': 'neptunedbinstance-xxjzfxbbtplk-3.c5pfn0sfmklq.eu-west-2.neptune.amazonaws.com',
        'status': 'rebooting',
        'endpointIsAvailable': False,
        'availabilityZone': 'eu-west-2b',
        'instanceType': 'db.r5.xlarge',
        'promotionTier': 5,
        'tags': [
          
        ]
      }
    ]

class TestParsing(unittest.TestCase):

    def test_implicit_equals(self): 
    
        specification1 = { 'role': 'writer' }
        specification2 = { 'role': 'reader' }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        
        self.assertEqual(instances_ids1, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-2'])
        
    def test_implicit_any_for_tags(self): 
    
        specification1 = { 'tags': [{ 'Key': 'Stack', 'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'}] }
        specification2 = { 'tags': [{ 'Key': 'Name', 'Value': 'another-app'}] }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        
        (instances_ids3, _) = get_instance_ids_for_specification(specification1, get_instances_without_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification2, get_instances_without_tags())
        
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids2, [])
        self.assertEqual(instances_ids3, [])
        self.assertEqual(instances_ids4, [])   
        
    def test_equals(self): 
    
        specification1 = { 'role': { 'equals': 'writer' } }
        specification2 = { 'role': { 'eq': 'writer' } }
        specification3 = { 'role': { 'equals': 'reader' } }
        specification4 = { 'role': { 'eq': 'reader' } }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification4, get_instances_with_tags())
        
        self.assertEqual(instances_ids1, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids3, ['neptunedbinstance-2'])
        self.assertEqual(instances_ids4, ['neptunedbinstance-2'])
        
    def test_boolean_equals(self):
        
        specification1 = { 'endpointIsAvailable': { 'eq': True } }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_without_tags())
        
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_not_equals(self): 
    
        specification1 = { 'role': { 'notEquals': 'reader' } }
        specification2 = { 'role': { 'notEq': 'reader' } }
        specification3 = { 'role': { 'notEquals': 'writer' } }
        specification4 = { 'role': { 'notEq': 'writer' } }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification4, get_instances_with_tags())
        
        self.assertEqual(instances_ids1, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids3, ['neptunedbinstance-2'])
        self.assertEqual(instances_ids4, ['neptunedbinstance-2'])
        
    def test_starts_with(self): 
    
        specification1 = { 'instanceId': { 'startsWith': 'neptunedb' } }
        specification2 = { 'instanceId': { 'startsWith': 'auroradb' } }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
                
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids2, [])
        
    def test_greater_than(self): 
    
        specification1 = { 'promotionTier': { 'gt': 1 } }
        specification2 = { 'promotionTier': { 'greaterThan': 1 } }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
       
        self.assertEqual(instances_ids1, ['neptunedbinstance-2'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-2'])
        
    def test_less_than(self): 
    
        specification1 = { 'promotionTier': { 'lt': 2 } }
        specification2 = { 'promotionTier': { 'lessThan': 2 } }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
       
        self.assertEqual(instances_ids1, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1'])
        
    def test_all(self): 
    
        specification1 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                    { 'Key': 'Stack', 'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'}
                ] 
            }
        }
        
        specification2 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                    { 'Key': 'Name', 'Value': 'Neptune-test'}
                 ] 
            }
        }
        
        specification3 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                    { 'Key': 'Name', 'Value': 'Neptune-prod'}
                 ] 
            }
        }
        
        specification4 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'}
                 ] 
            }
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification4, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids3, [])
        self.assertEqual(instances_ids4, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_any(self): 
    
        specification1 = { 'tags': 
            { 'any': [
                    { 'Key': 'Application', 'Value': 'AnotherApp'},
                    { 'Key': 'Stack', 'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'}
                ] 
            }
        }
        
        specification2 = { 'tags': 
            { 'any': [
                    { 'Key': 'Application', 'Value': 'AnotherApp'},
                    { 'Key': 'Name', 'Value': 'Neptune-test'}
                 ] 
            }
        }
        
        specification3 = { 'tags': 
            { 'any': [
                    { 'Key': 'Application', 'Value': 'AnotherApp'},
                    { 'Key': 'Name', 'Value': 'Neptune-prod'}
                 ] 
            }
        }
        
        specification4 = { 'tags': 
            { 'any': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'}
                 ] 
            }
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification4, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1'])
        self.assertEqual(instances_ids3, [])
        self.assertEqual(instances_ids4, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_any_for_simple_keys(self): 
    
        specification1 = { 'promotionTier': { 'any': [2, 5] } }
        specification2 = { 'status': { 'any': ['available', 'rebooting'] } }
        specification3 = { 'status': { 'any': ['modifying', 'upgrading'] } }
        specification4 = { 'status': { 'any': ['rebooting', 'upgrading'] } }
        
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_without_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_without_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_without_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification4, get_instances_without_tags())
        
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-2', 'neptunedbinstance-3'])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1', 'neptunedbinstance-2', 'neptunedbinstance-3'])
        self.assertEqual(instances_ids3, [])
        self.assertEqual(instances_ids4, ['neptunedbinstance-3'])
        
    
    def test_none(self): 
    
        specification1 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                    { 'Key': 'Stack', 'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'}
                ] 
            }
        }
        
        specification2 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': 'AnotherApp'},
                    { 'Key': 'Name', 'Value': 'Neptune-test'}
                 ] 
            }
        }
        
        specification3 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': 'AnotherApp'},
                    { 'Key': 'Name', 'Value': 'Neptune-prod'}
                 ] 
            }
        }
        
        specification4 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'}
                 ] 
            }
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
        (instances_ids4, _) = get_instance_ids_for_specification(specification4, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, [])
        self.assertEqual(instances_ids2, ['neptunedbinstance-2'])
        self.assertEqual(instances_ids3, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids4, [])
        
    def test_multiple_conditions_with_implicit_operators(self):
        
        specification1 = { 
            'role': 'writer',
            'status': 'available',
            'tags': [
                { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                { 'Key': 'Stack', 'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'}
            ]
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1'])
    
    def test_multiple_conditions_with_explicit_operators(self):
    
        specification1 = { 
            'role': {'any': ['writer', 'reader']},
            'status': {'eq': 'available'},
            'tags': {
                'all': [
                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                    { 'Key': 'Stack', 'Value': 'eu-west-2-NeptuneQuickStart-NeptuneStack-VJJA7BYK1MMB'}
                ]
            } 
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_root_or(self):
    
        specification1 = {
            'or': [
                { 
                    'role': 'writer',
                    'status': {'eq': 'available'},
                    'tags': {
                        'all': [
                            { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                            { 'Key': 'Name', 'Value': 'Neptune-test'}
                        ]
                    } 
                },
                { 
                    'role': 'reader',
                    'status': {'eq': 'available'},
                    'tags': {
                        'all': [
                            { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                            { 'Key': 'Name', 'Value': 'Neptune-demo'}
                        ]
                    } 
                }
                
            ]
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_root_and(self):
    
        specification1 = {
            'and': [
                {
                    'or': [
                        { 
                            'role': 'writer',
                            'tags': {
                                'all': [
                                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                                    { 'Key': 'Name', 'Value': 'Neptune-test'}
                                ]
                            } 
                        },
                        { 
                            'role': 'reader',
                            'tags': {
                                'all': [
                                    { 'Key': 'Application', 'Value': 'NeptuneCloudformation'},
                                    { 'Key': 'Name', 'Value': 'Neptune-demo'}
                                ]
                            } 
                        }
                        
                    ]
                },
                {
                    'status': 'available'
                }
            
            ]
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_json(self):
    
        specification1 = json.loads('''{
          "and": [
            {
              "or": [
                {
                  "role": "writer",
                  "tags": {
                    "all": [
                      {
                        "Key": "Application",
                        "Value": "NeptuneCloudformation"
                      },
                      {
                        "Key": "Name",
                        "Value": "Neptune-test"
                      }
                    ]
                  }
                },
                {
                  "role": "reader",
                  "tags": {
                    "all": [
                      {
                        "Key": "Application",
                        "Value": "NeptuneCloudformation"
                      },
                      {
                        "Key": "Name",
                        "Value": "Neptune-demo"
                      }
                    ]
                  }
                }
              ]
            },
            {
              "status": "available"
            }
          ]
        }''')
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
               
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        
    def test_all_with_complex_tag_criteria(self): 
    
        specification1 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'Neptune-'}}
                ] 
            }
        }
        
        specification2 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'NeptuneDB-'}}
                ] 
            }
        }
        
        specification3 = { 'tags': 
            { 'all': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'Neptune-d'} }
                ] 
            }
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
                       
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids2, [])
        self.assertEqual(instances_ids3, ['neptunedbinstance-2'])
        
    def test_any_with_complex_tag_criteria(self): 
    
        specification1 = { 'tags': 
            { 'any': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'MyTag', 'Value':  {'startsWith': 'Neptune-'}}
                ] 
            }
        }
        
        specification2 = { 'tags': 
            { 'any': [
                    { 'Key': 'FirstTag', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'MyTag', 'Value':  {'startsWith': 'NeptuneDB-'}}
                ] 
            }
        }
        
        specification3 = { 'tags': 
            { 'any': [
                    { 'Key': 'FirstTag', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'Neptune-d'} }
                ] 
            }
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
                       
        self.assertEqual(instances_ids1, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids2, [])
        self.assertEqual(instances_ids3, ['neptunedbinstance-2'])
        
    def test_none_with_complex_tag_criteria(self): 
    
        specification1 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'Neptune-'}}
                ] 
            }
        }
        
        specification2 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloudDB'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'NeptuneDB-'}}
                ] 
            }
        }
        
        specification3 = { 'tags': 
            { 'none': [
                    { 'Key': 'Application', 'Value': {'startsWith': 'NeptuneCloud'} },
                    { 'Key': 'Name', 'Value':  {'startsWith': 'Neptune-d'} }
                ] 
            }
        }
        
        (instances_ids1, _) = get_instance_ids_for_specification(specification1, get_instances_with_tags())
        (instances_ids2, _) = get_instance_ids_for_specification(specification2, get_instances_with_tags())
        (instances_ids3, _) = get_instance_ids_for_specification(specification3, get_instances_with_tags())
                       
        self.assertEqual(instances_ids1, [])
        self.assertEqual(instances_ids2, ['neptunedbinstance-1', 'neptunedbinstance-2'])
        self.assertEqual(instances_ids3, [])
        
        
        
if __name__ == '__main__':
    unittest.main()