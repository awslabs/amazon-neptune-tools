import sys, boto3, os

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

class NeptuneConnectionInfo:
    
    def __init__(self, glue_context):
        self.glue_context = glue_context
        
    def __neptune_connection(self, connection_name):
        proxy_url = self.glue_context._jvm.AWSConnectionUtils.getGlueProxyUrl()
        glue_endpoint = self.glue_context._jvm.AWSConnectionUtils.getGlueEndpoint()
        region = self.glue_context._jvm.AWSConnectionUtils.getRegion()
        if not proxy_url[8:].startswith('null'):
            os.environ['https_proxy'] = proxy_url
        glue = boto3.client('glue', endpoint_url=glue_endpoint, region_name=region)
        connection = glue.get_connection(Name=connection_name)
        del os.environ['https_proxy']
        return connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    
    def neptune_endpoint(self, connection_name):
        """Gets Neptune endpoint information from the Glue Data Catalog.
        
        You can store Neptune endpoint information as JDBC connections in the Glue Data Catalog.
        JDBC connection strings must begin 'jdbc:'. To store a Neptune endpoint, use the following format:
        
        'jdbc:<protocol>://<dns_name>:<port>/<endpoint>'
        
        For example, if you store:
        
        'jdbc:ws://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin'
        
        – this method will return:
        
        'ws://my-neptune-cluster.us-east-1.neptune.amazonaws.com:8182/gremlin' 
        
        Example:
        >>> gremlin_endpoint = NeptuneConnectionInfo(glueContext).neptune_endpoint('neptune')
        """
        return self.__neptune_connection(connection_name)[5:]