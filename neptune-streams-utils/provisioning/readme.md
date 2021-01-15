#provision-neptune-streams-handler

Provisions a Neptune Streams handler.

### Prerequisites

```
pip install boto3
pip install fire
pip install tabulate
```

Before provisioning a handler using this script ensure the following conditions are met:
  
  - You have an existing Neptune cluster
  - Neptune Streams is [enabled](https://docs.aws.amazon.com/neptune/latest/userguide/streams-using.html#streams-using-enabling)
  
### Usage

```
NAME
    provision_neptune_streams_handler.py

SYNOPSIS
    provision_neptune_streams_handler.py CLUSTER_ID HANDLER_S3_BUCKET HANDLER_S3_KEY <flags>

POSITIONAL ARGUMENTS
    CLUSTER_ID
    HANDLER_S3_BUCKET
    HANDLER_S3_KEY

FLAGS
    --handler_name=HANDLER_NAME
    --additional_params=ADDITIONAL_PARAMS
    --query_engine=QUERY_ENGINE
    --region=REGION
    --lambda_memory_size_mb=LAMBDA_MEMORY_SIZE_MB
    --lambda_runtime=LAMBDA_RUNTIME
    --lambda_logging_level=LAMBDA_LOGGING_LEVEL
    --managed_policy_arns=MANAGED_POLICY_ARNS
    --batch_size=BATCH_SIZE
    --max_polling_wait_time_seconds=MAX_POLLING_WAIT_TIME_SECONDS
    --max_polling_interval_seconds=MAX_POLLING_INTERVAL_SECONDS
    --step_function_fallback_period=STEP_FUNCTION_FALLBACK_PERIOD
    --step_function_fallback_period_unit=STEP_FUNCTION_FALLBACK_PERIOD_UNIT
    --notification_email=NOTIFICATION_EMAIL
    --create_cloudwatch_alarm=CREATE_CLOUDWATCH_ALARM
    --application_name=APPLICATION_NAME
    --dry_run=DRY_RUN

NOTES
    You can also use flags syntax for POSITIONAL ARGUMENTS
```

### Examples

Here's an example that provisions a handler with the default handler name (`stream_handler.StreamHandler`), and which has been uploaded to _s3://my-bucket/handlers/example_handler.zip_:

```
python provision_neptune_streams_handler.py --cluster_id=neptunedbcluster-xyz0a0a0abc \
  --handler_s3_bucket=my-bucket \
  --handler_s3_key=handlers/example_handler.zip \
  --region=us-east-1
```

Here's an example of using the script to install a handler with an additional parameter (`delivery_stream_name`, which will be supplied to the handler via an environment variable when it is invoked), and a managed policy that allows the handler to invoke an Amazon Kinesis Data Firehose API:

```
python provision_neptune_streams_handler.py \
  --cluster_id=neptunedbcluster-abcdefghijkl \
  --handler_s3_bucket=my-bucket \
  --handler_s3_key=neptune_firehose_handler.zip \
  --additional_params='{"delivery_stream_name": "neptune-firehose"}' \
  --managed_policy_arns='["arn:aws:iam::123456789:policy/neptune-firehose-handler-policy"]' \
  --region=us-east-1
```