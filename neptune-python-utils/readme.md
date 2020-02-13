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
 

## Using neptune-python-utils with AWS Glue

### Examples

See [Migrating from MySQL to Amazon Neptune using AWS Glue](https://github.com/aws-samples/amazon-neptune-samples/tree/master/gremlin/glue-neptune).
 
### Cross Account/Region Datasources
If you have a datasource in a different region and/or different account from Glue and your Neptune database, you can follow the instructions in this [blog](https://aws.amazon.com/blogs/big-data/create-cross-account-and-cross-region-aws-glue-connections/) to allow access.
 

 
