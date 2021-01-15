#streams-to-firehose

This Neptune Streams handler publishes Neptune Streams records to an Amazon Kinesis Data Firehose.

## Installing

  1. Build the handler using the _build.sh_ file.
  2. Upload the _neptune_firehose_handler.zip_ file to an S3 bucket.
  3. Create a Kinesis Data Firehose delivery stream called 'neptune-firehose'.
  4. Create an IAM policy called 'neptune-firehose-handler-policy' using the snippet below, replacing the `<region>` and `<account>` placeholders.
  5. Provision the handler using the _provision_neptune_streams_handler.py_ script in the _provisioning_ folder. Ensure you supply the correct delivery stream name and IAM policy ARN.
  

### Example IAM policy
  
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "firehose:PutRecordBatch",
            "Resource": "arn:aws:firehose:<region>:<account>:deliverystream/neptune-firehose"
        }
    ]
}
```

### Example provisioning command

Here's an example of using the script to install the handler (after having built the handler and uploading it in S3):

```
python provision_neptune_streams_handler.py \
  --cluster_id=neptunedbcluster-abcdefghijkl \
  --handler_s3_bucket=my-bucket \
  --handler_s3_key=neptune_firehose_handler.zip \
  --additional_params='{"delivery_stream_name": "neptune-firehose"}' \
  --managed_policy_arns='["arn:aws:iam::123456789:policy/neptune-firehose-handler-policy"]' \
  --region=us-east-1
```
