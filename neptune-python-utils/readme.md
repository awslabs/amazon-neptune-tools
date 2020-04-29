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

GremlinUtils.init_statics(globals())

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

GremlinUtils.init_statics(globals())

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

GremlinUtils.init_statics(globals())

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

GremlinUtils.init_statics(globals())

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

Create a Neptune access IAM role with a policy that allows connections to your Neptune database using IAM database authentication. For example:

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

If your Neptune access IAM role as created in Step 1. has the ARN `arn:aws:iam::111111111111:role/GlueConnectToNeptuneRole`, attach the following inline policy to your Glue job's IAM role:

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

Assuming your Neptune access IAM role as created in Step 1. has the ARN `arn:aws:iam::111111111111:role/GlueConnectToNeptuneRole`, you can assume the role like this:

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

Credentials generated via `sts.assume_role()` last an hour. If you have a long running Glue job, you may want to create a `RefreshableCredentials` object. See [this article](https://dev.to/li_chastina/auto-refresh-aws-tokens-using-iam-role-and-boto3-2cjf) for more details.

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

region = args['AWS_REGION']
role_arn = args['CONNECT_TO_NEPTUNE_ROLE_ARN']

endpoints = GlueNeptuneConnectionInfo(region, role_arn).neptune_endpoints('neptune-db')
```

### Using neptune-python-utils to insert or upsert data from an AWS Glue job

The code below, taken from the sample Glue job [export-from-mysql-to-neptune.py](https://github.com/aws-samples/amazon-neptune-samples/blob/master/gremlin/glue-neptune/glue-jobs/mysql-neptune/export-from-mysql-to-neptune.py), shows extracting data from several tables in an RDBMS, formatting the dynamic frame columns according to the Neptune bulk load CSV column headings format, and then bulk loading direct into Neptune.

Parallel inserts and upserts can sometimes trigger a `ConcurrentModifcationException`. _neptune-python-utils_ will attempt 5 retries for each batch should such exceptions occur. 

```
import sys, boto3, os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.transforms import RenameField
from awsglue.transforms import SelectFields
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import format_string
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import *
from neptune_python_utils.glue_neptune_connection_info import GlueNeptuneConnectionInfo
from neptune_python_utils.glue_gremlin_client import GlueGremlinClient
from neptune_python_utils.glue_gremlin_csv_transforms import GlueGremlinCsvTransforms
from neptune_python_utils.endpoints import Endpoints
from neptune_python_utils.gremlin_utils import GremlinUtils

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'NEPTUNE_CONNECTION_NAME', 'AWS_REGION', 'CONNECT_TO_NEPTUNE_ROLE_ARN'])

sc = SparkContext()
glueContext = GlueContext(sc)
 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database = args['DATABASE_NAME']
product_table = 'salesdb_product'
product_category_table = 'salesdb_product_category'
supplier_table = 'salesdb_supplier'

# Create Gremlin client

gremlin_endpoints = GlueNeptuneConnectionInfo(args['AWS_REGION'], args['CONNECT_TO_NEPTUNE_ROLE_ARN']).neptune_endpoints(args['NEPTUNE_CONNECTION_NAME'])
gremlin_client = GlueGremlinClient(gremlin_endpoints)

# Create Product vertices

print("Creating Product vertices...")

# 1. Get data from source SQL database
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = product_table, transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = product_category_table, transformation_ctx = "datasource1")
datasource2 = datasource0.join( ["CATEGORY_ID"],["CATEGORY_ID"], datasource1, transformation_ctx = "join")

# 2. Map fields to bulk load CSV column headings format
applymapping1 = ApplyMapping.apply(frame = datasource2, mappings = [("NAME", "string", "name:String", "string"), ("UNIT_PRICE", "decimal(10,2)", "unitPrice", "string"), ("PRODUCT_ID", "int", "productId", "int"), ("QUANTITY_PER_UNIT", "int", "quantityPerUnit:Int", "int"), ("CATEGORY_ID", "int", "category_id", "int"), ("SUPPLIER_ID", "int", "supplierId", "int"), ("CATEGORY_NAME", "string", "category:String", "string"), ("DESCRIPTION", "string", "description:String", "string"), ("IMAGE_URL", "string", "imageUrl:String", "string")], transformation_ctx = "applymapping1")

# 3. Append prefixes to values in ID columns (ensures vertices for diffferent types have unique IDs across graph)
applymapping1 = GlueGremlinCsvTransforms.create_prefixed_columns(applymapping1, [('~id', 'productId', 'p'),('~to', 'supplierId', 's')])

# 4. Select fields for upsert
selectfields1 = SelectFields.apply(frame = applymapping1, paths = ["~id", "name:String", "category:String", "description:String", "unitPrice", "quantityPerUnit:Int", "imageUrl:String"], transformation_ctx = "selectfields1")

# 5. Upsert batches of vertices
selectfields1.toDF().foreachPartition(gremlin_client.upsert_vertices('Product', batch_size=100))

# Create Supplier vertices

print("Creating Supplier vertices...")

# 1. Get data from source SQL database
datasource3 = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = supplier_table, transformation_ctx = "datasource3")

# 2. Map fields to bulk load CSV column headings format
applymapping2 = ApplyMapping.apply(frame = datasource3, mappings = [("COUNTRY", "string", "country:String", "string"), ("ADDRESS", "string", "address:String", "string"), ("NAME", "string", "name:String", "string"), ("STATE", "string", "state:String", "string"), ("SUPPLIER_ID", "int", "supplierId", "int"), ("CITY", "string", "city:String", "string"), ("PHONE", "string", "phone:String", "string")], transformation_ctx = "applymapping1")

# 3. Append prefixes to values in ID columns (ensures vertices for diffferent types have unique IDs across graph)
applymapping2 = GlueGremlinCsvTransforms.create_prefixed_columns(applymapping2, [('~id', 'supplierId', 's')])

# 4. Select fields for upsert
selectfields3 = SelectFields.apply(frame = applymapping2, paths = ["~id", "country:String", "address:String", "city:String", "phone:String", "name:String", "state:String"], transformation_ctx = "selectfields3")

# 5. Upsert batches of vertices
selectfields3.toDF().foreachPartition(gremlin_client.upsert_vertices('Supplier', batch_size=100))

# SUPPLIER edges

print("Creating SUPPLIER edges...")

# 1. Reuse existing DF, but rename ~id column to ~from
applymapping1 = RenameField.apply(applymapping1, "~id", "~from")

# 2. Create unique edge IDs
applymapping1 = GlueGremlinCsvTransforms.create_edge_id_column(applymapping1, '~from', '~to')

# 3. Select fields for upsert
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["~id", "~from", "~to"], transformation_ctx = "selectfields2")

# 4. Upsert batches of edges
selectfields2.toDF().foreachPartition(gremlin_client.upsert_edges('SUPPLIER', batch_size=100))

# End

job.commit()

print("Done")
```

### Further Examples

See [Migrating from MySQL to Amazon Neptune using AWS Glue](https://github.com/aws-samples/amazon-neptune-samples/tree/master/gremlin/glue-neptune).
 
### Cross Account/Region Datasources
If you have a datasource in a different region and/or different account from Glue and your Neptune database, you can follow the instructions in this [blog](https://aws.amazon.com/blogs/big-data/create-cross-account-and-cross-region-aws-glue-connections/) to allow access.
 

 
