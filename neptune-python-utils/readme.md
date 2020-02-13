# neptune-python-utils

_neptune-python-utils_ is a Python 3 library that simplifies using [Gremlin-Python](https://pypi.org/project/gremlinpython/) to connect to Amazon Neptune. The library makes it easy to configure your driver to support [IAM DB Authentication](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth.html), create sessioned interactions with Neptune, and write data to Amazon Neptune from AWS Glue jobs.

You can use _neptune-python-utils_ in AWS Lambda functions, Jupyter notebooks, AWS Glue PySpark and Python shell jobs, and in your own Python applications.

With _neptune-python-utils_ you can:

 - Connect to Neptune using [IAM DB Authentication](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth.html)
 - Trigger and monitor [bulk load operations](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html)
 - Create a [sessioned client](https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-gremlin-sessions.html) for implicit transactions that span multiple requests
 - Get Neptune connection information from the Glue Data Catalog
 - Create label and node and edge ID columns in DynamicFrames, named in accordance with the Neptune CSV bulk load format for property graphs
 - Write from DynamicFrames directly to Neptune 
 
## Build

`sh build.sh`

This creates a zip file: `target/neptune_python_utils.zip`. 

When using AWS Glue to write data to Neptune, copy the zip file to an S3 bucket. You can then refer to _neptune-python-utils_ from your Glue Development Endpoint or Glue job. See [Using Python Libraries with AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html). 

## Examples

### Querying

The following query uses `NEPTUNE_CLUSTER_ENDPOINT` and `NEPTUNE_CLUSTER_PORT` environment variables to create a connection to the Gremlin endpoint. It automatically uses the credntial provider chain to connect to the database if IAM DB Auth is enabled.

```
from neptune_python_utils.gremlin_utils import GremlinUtils

gremlin_utils = GremlinUtils()

conn = gremlin_utils.remote_connection()
g = gremlin_utils.traversal_source(connection=conn)

print(g.V().limit(10).valueMap().toList())

conn.close()
```

If you want to supply your own endpoint information, you can use `neptune_endpoint` and `neptune_port` parameters to an `Endpoints` object:

```
from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints

endpoints = Endpoints(neptune_endpoint='demo.cluster-111222333.eu-west-2.neptune.amazonaws.com')
gremlin_utils = GremlinUtils(endpoints)

conn = gremlin_utils.remote_connection()
g = gremlin_utils.traversal_source(connection=conn)

print(g.V().limit(10).valueMap().toList())

conn.close()
```

If you want to supply your own credentials, you can supply a `Credentials` object to the `Endpoints` object. Here we're simply getting the credentials from the session (you don't normally have to do this – _neptune-python-utils_ will get credentials from the provider chain automatically).

```
from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints
import boto3

session = boto3.session.Session()
credentials = session.get_credentials()

endpoints = Endpoints(
    neptune_endpoint='demo.cluster-111222333.eu-west-2.neptune.amazonaws.com',
    credentials=credentials)

gremlin_utils = GremlinUtils(endpoints)

conn = gremlin_utils.remote_connection()
g = gremlin_utils.traversal_source(connection=conn)

print(g.V().limit(10).valueMap().toList())

conn.close()
```

### Sessioned client

The following code creates a sessioned client. All requests sent using this client will be executed in a single implicit transaction. The transaction will commit when the sessioned client is close (we're using a `with` block here to close the session). The transaction will be rolled back if an exception occurs:

```
from neptune_python_utils.gremlin_utils import GremlinUtils

gremlin_utils = GremlinUtils()

try:
    with gremlin_utils.sessioned_client() as client:
        client.submit("g.addV('User').property(T.id, 'person-x')").all().result()
        client.submit("g.addV('User').property(T.id, 'person-y')").all().result()
        client.submit("g.V('person-x').addE('KNOWS').to(V('person-y'))").all().result()
except Exception as err:
    print('Error: {}'.format(err))
    print('Rolling back session')
    
g = gremlin_utils.traversal_source()

print(g.V('person-x').outE('KNOWS').count().next())

gremlin_utils.close()
``` 

### Bulk loading data into Neptune

`BulkLoad` automatically supports IAM DB Auth, just as `GremlinUtils` does. You can supply an `Endpoints` object with custom credentials to the `endpoints` parameter of the `BulkLoad` constructor if necessary.

To bulk load and block until the load is complete:

```
from neptune_python_utils.bulkload import BulkLoad

bulkload = BulkLoad(
	source='s3://...', 
	update_single_cardinality_properties=True)
bulkload.load()
```

Alternatively you can invoke the load and check the status using the returned `BulkLoadStatus` object:

```
from neptune_python_utils.bulkload import BulkLoad

bulkload = BulkLoad(
	source='s3://ianrob-neptune-lhr/mysql-to-neptune/', 
	update_single_cardinality_properties=True)
load_status = bulkload.load_async()

status, json = load_status.status(details=True, errors=True)
print(json)

load_status.wait()
```

## Using neptune-python-utils with AWS Glue

### Connecting to Neptune from an AWS Glue job using IAM DB Auth

To connect to an IAM DB Auth-enabled Neptune database from an AWS Glue job, complete the following steps:

#### 1. Create a Neptune access role that your AWS Glue job can assume

Create an IAM Neptune access IAM role with a policy that allows connections to your Neptune database using IAM database authentication. For example:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "neptune-db:connect",
            "Resource": "arn:aws:neptune-db:eu-west-1:111111111111:*/*",
            "Effect": "Allow"
        }
    ]
}
```

Instead of `*/*` you should consider restricting access to a specific cluster. See [Creating and Using an IAM Policy for IAM Database Access](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth.html#iam-auth-policy) for more details.

#### 2. Create a trust relationship that allows the Neptune access role to be assumed by your Glue job's IAM role

If your AWS Glue job runs with the `MyGlueIAMRole` IAM role, then create a trust relationship attached to the Neptune access role created in Step 1. that looks like this:

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::111111111111:role/MyGlueIAMRole"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### 3. Attach a policy to your Glue job's IAM role allowing it to assume the Neptune access role

If your Neptune access IAM role, as created in Step 1., has the ARN `arn:aws:iam::111111111111:role/GlueConnectToNeptuneRole`, attach the following inline policy to your Glue job's IAM role:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::111111111111:role/GlueConnectToNeptuneRole",
            "Effect": "Allow"
        }
    ]
}
```

#### 4. In your PySpark job or Python shell script, assume the access role and create a Credentials object

Assuming your Neptune access IAM role, as created in Step 1., has the ARN `arn:aws:iam::111111111111:role/GlueConnectToNeptuneRole`, you can assume the role like this:

```
import boto3, uuid
from botocore.credentials import Credentials

region = 'eu-west-1'
role_arn = 'arn:aws:iam::111111111111:role/GlueConnectToNeptuneRole'

sts = boto3.client('sts', region_name=region)

role = sts.assume_role(
    RoleArn=role_arn,
    RoleSessionName=uuid.uuid4().hex,
    DurationSeconds=3600
)

credentials = Credentials(
    access_key=role['Credentials']['AccessKeyId'], 
    secret_key=role['Credentials']['SecretAccessKey'], 
    token=role['Credentials']['SessionToken'])
```

This `Credentials` object can then be passed to an `Endpoints` object:

```
from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints
import boto3

session = boto3.session.Session()
credentials = session.get_credentials()

endpoints = Endpoints(credentials=credentials)

gremlin_utils = GremlinUtils(endpoints)

conn = gremlin_utils.remote_connection()
g = gremlin_utils.traversal_source(connection=conn)

print(g.V().limit(10).valueMap().toList())

conn.close()
```

If using a `GlueNeptuneConnectionInfo` object to get Neptune connection information from the Glue Data Catalog, simply pass the region and Neptune access IAM role ARN to the `GlueNeptuneConnectionInfo` constructor:

```
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from neptune_python_utils.glue_neptune_connection_info import GlueNeptuneConnectionInfo
from neptune_python_utils.endpoints import Endpoints

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'AWS_REGION', 'CONNECT_TO_NEPTUNE_ROLE_ARN'])

sc = SparkContext()
glueContext = GlueContext(sc)
 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

endpoints = GlueNeptuneConnectionInfo(args['AWS_REGION'], args['CONNECT_TO_NEPTUNE_ROLE_ARN']).neptune_endpoints('neptune-db')
```

### Examples

See [Migrating from MySQL to Amazon Neptune using AWS Glue](https://github.com/aws-samples/amazon-neptune-samples/tree/master/gremlin/glue-neptune).
 
### Cross Account/Region Datasources
If you have a datasource in a different region and/or different account from Glue and your Neptune database, you can follow the instructions in this [blog](https://aws.amazon.com/blogs/big-data/create-cross-account-and-cross-region-aws-glue-connections/) to allow access.
 

 
