# glue-neptune

__Updated Feb 2020: This library is now deprecated in favour of [_neptune-python-utils_](https://github.com/awslabs/amazon-neptune-tools/tree/master/neptune-python-utils)__

_glue-neptune_ is a Python library for AWS Glue that helps writing data to Amazon Neptune from Glue jobs.

With _glue-neptune_ you can:

 - Get Neptune connection information from the Glue Data Catalog
 - Create label and node and edge ID columns in DynamicFrames, named in accordance with the Neptune CSV bulk load format for property graphs
 - Write from DynamicFrames directly to Neptune 
 
## Build

`sh build.sh`

This creates a zip file: `target/glue_neptune.zip`. Copy this zip file to an S3 bucket.

You can then refer to this library from your Glue Development Endpoint or Glue job. See [Using Python Libraries with AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html). 
 
## Examples

See [Migrating from MySQL to Amazon Neptune using AWS Glue](https://github.com/aws-samples/amazon-neptune-samples/tree/master/gremlin/glue-neptune).
 
## Cross Account/Region Datasources
If you have a datasource in a different region and/or different account from Glue and your Neptune database, you can follow the instructions in this [blog](https://aws.amazon.com/blogs/big-data/create-cross-account-and-cross-region-aws-glue-connections/) to allow access.
 

 
