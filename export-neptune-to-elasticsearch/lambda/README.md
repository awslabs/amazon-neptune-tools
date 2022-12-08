# Export Neptune to Elasticsearch Lambda Functions

This stack deploys two lambda functions:
- export-neptune-to-kinesis-\<stackId\>
- kinesis-to-elasticsearch-\<stackId\>

Both lambda functions leverage the same lambda code package, yet use different handler configurations.  The following instructions detail how to build the unified code package for both of these lambda functions.  This uses an included `build.sh` script to create a target ZIP file that can be directly pushed to AWS Lambda.

NOTE:  Each lambda function also has dependencies on other Lambda Layers.  The Lambda Layers are not part of this code repository and are deployed as part of the base [Neptune Streams Poller stack](https://docs.aws.amazon.com/neptune/latest/userguide/full-text-search-cfn-create.html).

## Build

The entire package can be built using the following:

`sh build.sh`

This will create a new `target` directory with the ZIP file package used in both lambda functions.

To update either lambda function, you can use:

`aws lambda update-function-code --function-name export-neptune-to-kinesis-<stackId> --zip-file fileb://./target/export-neptune-to-elasticsearch.zip`

or

`aws lambda update-function-code --function-name kinesis-to-elasticsearch-<stackId> --zip-file fileb://./target/export-neptune-to-elasticsearch.zip`

