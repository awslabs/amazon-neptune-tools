#neptune-stream-utils

This project includes example [Neptune Streams](https://docs.aws.amazon.com/neptune/latest/userguide/streams.html) handlers and build scripts, and a command-line tool that installs a handler in the Neptune Streams polling framework.

## Example handlers

The _examples_ directory contains sample handlers written in Python and Java, and scripts for building them. You can use these as the basis of your own handlers. Run _build.sh_, and then copy the zip file in the _target_ directory to your S3 bucket.

### Java handler dependencies

The Java handler depends on two libraries that are not currently available in Maven. Before building the Java hander, run the _install-dependencies.sh_ script to install the libraries in your local Maven repository.

## Provisioning script

The _provisioning_ folder contains a command-line tool that installs a handler in the Neptune Streams polling framework. The handler and polling framework are created using a [CloudFormation template](https://s3.amazonaws.com/aws-neptune-customer-samples/neptune-stream/neptune_stream_poller_nested_full_stack.json) provided by Neptune. This CloudFormation template has over 25 input parameters. The script here simplifies running the CloudFormation template. The script queries the AWS Management APIs to get details of the Neptune cluster, VPC, subnets, security groups, etc, and then populates and invokes the CloudFormation template.

Here's an example of using the script to install the example Python handler (after having built the handler and putting it in S3):

```
python provision_neptune_streams_handler.py \
  --cluster_id=neptunedbcluster-abcdefghijkl \
  --handler_s3_bucket=my-bucket \
  --handler_s3_key=streams/stream_handler.zip \
  --region=us-east-1
```

Here's an example of using the script to install the example Java handler (after having built the handler and putting it in S3):

```
python provision_neptune_streams_handler.py \
  --cluster_id=neptunedbcluster-abcdefghijkl \
  --handler_s3_bucket=my-bucket \
  --handler_s3_key=streams/stream_handler.jar \
  --lambda_runtime=java8 \
  --region=us-east-1
```

If you supply an additional `–dry_run=true` parameter, the tool will simply create all the CloudFormation parameters, but not actually run the template.

## Additional resources

More details on creating your own custom handlers can be found in the blog post [Capture graph changes using Neptune Streams](https://aws.amazon.com/blogs/database/capture-graph-changes-using-neptune-streams/).

