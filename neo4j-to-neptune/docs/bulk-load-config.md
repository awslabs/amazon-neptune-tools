# Bulk Load Configuration

The `convert-csv` utility supports automated bulk loading of converted CSV data directly into Amazon Neptune using the `--bulk-load-config` parameter.

## Usage

Use the `--bulk-load-config` parameter to specify a YAML file containing the bulk load configuration:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /tmp/neo4j-export.csv \
  -d output \
  --bulk-load-config bulk-load.yaml
```

## YAML Configuration Format

The configuration file should be in YAML format, using camelCase:

```yaml
# Required parameters
bucketName: my-neptune-data-bucket
neptuneEndpoint: my-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com
iamRoleArn: arn:aws:iam::123456789012:role/NeptuneLoadFromS3Role

# Optional parameters
s3Prefix: neptune
parallelism: OVERSUBSCRIBE
monitor: true
```

## Configuration Parameters

### Required Parameters

- **`bucketName`**: S3 bucket name for CSV file storage
- **`neptuneEndpoint`**: Neptune cluster endpoint URL
- **`iamRoleArn`**: IAM role ARN with S3 and Neptune permissions

### Optional Parameters

- **`s3Prefix`**: S3 prefix for uploaded files
- **`parallelism`**: Load parallelism level - `LOW`, `MEDIUM`, `HIGH`, `OVERSUBSCRIBE` (default: `OVERSUBSCRIBE`)
- **`monitor`**: Monitor load progress until completion (default: `false`)

## Command Line Override

Individual CLI parameters can override configuration file values:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /tmp/neo4j-export.csv \
  -d output \
  --bulk-load-config bulk-load.yaml \
  --bucket-name override-bucket \
  --parallelism HIGH
```

Available override parameters:
- `--bucket-name`
- `--s3-prefix`
- `--neptune-endpoint`
- `--iam-role-arn`
- `--parallelism`
- `--monitor`

## Behavior

- **Optional**: Bulk loading only occurs when `--bulk-load-config` or `--neptune-endpoint` is provided
- **Validation**: All required parameters are validated before conversion begins
- **Process**: Conversion happens first, then files are uploaded to S3 and bulk load is initiated
- **Monitoring**: When enabled, the tool waits and reports progress until completion

## Example Output

```
Vertices: 171
Edges   : 253
Output  : output/1751656971039
output/1751656971039

Completed in x second(s)
S3 Bucket: my-bucket
S3 Prefix: neptune
AWS Region: us-east-2
IAM Role ARN: arn:aws:iam::123456789000:role/NeptunePolicy
Neptune Endpoint: my-neptune-db.cluster-xxxxxxxxxxxx.us-east-2.neptune.amazonaws.com
Bulk Load Parallelism: MEDIUM
Uploading Gremlin load data to S3...
Starting async upload of files from /tmp/output/1751656971039 to s3://my-bucket/neptune/1751656971039
Starting async upload of /tmp/output/1751656971039/vertices.csv to s3://my-bucket/neptune/1751656971039/vertices.csv
Starting async upload of /tmp/output/1751656971039/edges.csv to s3://my-bucket/neptune/1751656971039/edges.csv
Successfully uploaded vertices.csv - ETag: "abc123..."
Successfully uploaded edges.csv - ETag: "def456..."
Successfully uploaded 2 files from /tmp/output/1751656971039
Files uploaded successfully to S3. Files available at: s3://my-bucket/neptune/1751656971039/
Starting Neptune bulk load...
Testing connectivity to Neptune endpoint...
Successful connected to Neptune. Status: 200 healthy
Neptune bulk load started successfully! Load ID: 12345678-1234-1234-1234-123456789012
Monitoring load progress for job: 12345678-1234-1234-1234-123456789012
Neptune bulk load status: LOAD_IN_PROGRESS
Neptune bulk load status: LOAD_IN_PROGRESS
Neptune bulk load completed with status: LOAD_COMPLETED
```
