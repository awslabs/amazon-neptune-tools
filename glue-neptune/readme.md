# glue-neptune

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

The _examples_ directory contains 3 example Glue jobs:

 - _export-from-mysql-to-s3_ – Shows how to export from several MySQL tables to CSV formatted in accordance with the Neptune CSV bulk load format. These files are saved to S3, ready to be bulk loaded into Neptune.
 - _export-from-mysql-to-neptune_ – Shows how to export direct from several MySQL tables into Neptune. Nodes and edges are written conditionally to the database using user-supplied IDs.
 - _export-from-mysql-to-neptune-incremental_ – Shows how to perform an incremental  load from MySQL to Neptune using checkpoint information that is written to a Neptune vertex.
 
 ## Cross Account/Region Datasources
If you have a datasource in a different region and/or different account from Glue and your Neptune database, you can follow the instructions in this [blog](https://aws.amazon.com/blogs/big-data/create-cross-account-and-cross-region-aws-glue-connections/) to allow access.
 

 
