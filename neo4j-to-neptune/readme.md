# neo4j-to-neptune

A command-line utility for migrating data from Neo4j to Neptune.

### Examples

**Conversion only:**
```bash
java -jar neo4j-to-neptune.jar convert-csv -i /tmp/neo4j-export.csv -d output --infer-types
```

**Convert and bulk load to Neptune:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /tmp/neo4j-export.csv \
  -d output \
  --bulk-load \
  --bucket-name my-neptune-bucket \
  --neptune-endpoint my-cluster.cluster-abc123.us-east-2.neptune.amazonaws.com \
  --iam-role-arn arn:aws:iam::123456789012:role/NeptuneLoadFromS3 \
  --infer-types
```

### Build

```
mvn clean install
```

### Usage

  - [`convert-csv`](docs/convert-csv.md)

## Migration Process

Migration of data from Neo4j to Neptune can now be accomplished in two ways:

### Option 1: Manual Multi-Step Process

 1. [**Export CSV from Neo4j**](#export-csv-from-neo4j) – Use the APOC export procedures to export data from Neo4j to CSV.
 2. [**Convert CSV**](#convert-csv) – Use the `convert-csv` command-line utility to convert the exported CSV into the Neptune Gremlin bulk load CSV format.
 3. [**Bulk Load into Neptune**](#bulk-load-into-neptune-manual) – Use the Neptune bulk load API to load data into Neptune.

### Option 2: Automated End-to-End Process

 1. [**Export CSV from Neo4j**](#export-csv-from-neo4j) – Use the APOC export procedures to export data from Neo4j to CSV.
 2. [**Convert and Bulk Load**](#convert-and-bulk-load) – Use the `convert-csv` command with `--bulk-load` flag to automatically convert CSV, upload to S3, and load into Neptune.

### Export CSV from Neo4j

Use the [`apoc.export.csv.all`](https://neo4j.com/docs/labs/apoc/current/export/csv/) procedure from neo4j's [APOC](https://neo4j.com/docs/labs/apoc/current/) library to export data from Neo4j to CSV.

#### 1. Install APOC

Follow the [instructions](https://neo4j.com/docs/labs/apoc/current/introduction/) for installing the APOC library for either Neo4j Desktop or Neo4j Server.

#### 2. Enable exports

Update the _neo4j.conf_ configuration file to enable exports:

```
apoc.export.file.enabled=true
```

#### 3. Export to CSV

```
CALL apoc.export.csv.all(
 "neo4j-export.csv",
 {d:','}
)
```

**Note:** When running this command please use the syntax above, do not include `{stream:true}` in the command.  Streaming the results back to the browser and then downloading them as a CSV will result in a file that will not be correctly processed by the conversion utility.

The path that you specify for the export file will be resolved relative to the Neo4j _import_ directory. `apoc.export.csv.all` creates a single CSV file containing data for all nodes and relationships.

### Convert CSV

Use the [`convert-csv`](docs/convert-csv.md) command-line utility to convert the CSV exported from Neo4j into the Neptune Gremlin bulk load CSV format.

The utility has two required parameters: the path to the Neo4j export file and the name of a directory where the converted CSV files will be written. There are also optional parameters that allow you to specify node and relationship multi-valued property policies and turn on data type inferencing.

#### Multi-valued property policies

Neo4j allows ['homogeneous lists of simple types'](https://neo4j.com/docs/cypher-manual/current/syntax/values/) to be stored as properties on both nodes and edges. These lists can contain duplicate values.

Neptune provides for set and single cardinality for vertex properties, and single cardinality for edge properties. Hence, there is no straightforward migration of Neo4j node list properties containing duplicate values into Neptune vertex properties, or Neo4j relationship list properties into Neptune edge properties.

The `--node-property-policy` and `--relationship-property-policy` parameters allow you to control the migration of multi-valued properties into Neptune.

`--node-property-policy` takes one of four values, the default being `PutInSetIgnoringDuplicates`:

  - `LeaveAsString` – Store a multi-valued Neo4j node property as a string representation of a JSON-formatted list
  - `Halt` – Halt (throw an exception) if a multi-valued Neo4j node property is encountered
  - `PutInSetIgnoringDuplicates` – Convert a multi-valued Neo4j node property to a set cardinality Neptune property, discarding duplicate values
  - `PutInSetButHaltIfDuplicates` – Convert a multi-valued Neo4j node property to a set cardinality Neptune property, discarding duplicate values but halt (throw an exception) if a multi-valued Neo4j node property containing duplicate values is encountered

`--relationship-property-policy` takes one of two values, the default being `LeaveAsString`:

  - `LeaveAsString` – Store a multi-valued Neo4j relationship property as a string representation of a JSON-formatted list
  - `Halt` – Halt (throw an exception) if a multi-valued Neo4j relationship property is encountered

#### Data type inferencing

When importing data into Neptune using the bulk loader, you can [specify the data type for each property](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html). If you supply an `--infer-types` flag to `convert-csv`, the utility will attempt to infer the narrowest supported type for each column in the output CSV.

Note that `convert-csv` will always use a __double__ for values with decimal or scientific notation.

### Bulk Load into Neptune

Use the [Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html) to load data into Neptune from the converted CSV files.

### Convert and Bulk Load

The `convert-csv` command now supports an integrated bulk loading option that automatically uploads converted CSV files to S3 and initiates the Neptune bulk load process.

#### Prerequisites

Before using the bulk load feature, ensure you have:

1. **AWS Credentials**: Configure AWS credentials using one of the following methods:
   - AWS CLI: `aws configure`
   - Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   - IAM roles (if running on EC2)
   - AWS credentials file

2. **S3 Bucket**: An S3 bucket where CSV files will be uploaded, where the region is the same as your Neptune Cluster

3. **IAM Role**: An IAM role with permissions to:
   - Read from the S3 bucket
   - Access Neptune cluster
   - Perform bulk load operations

4. **Neptune Cluster**: A running Neptune cluster with bulk load enabled, where the region is the same as your S3 bucket

#### Usage

To enable bulk load, add the `--bulk-load` flag along with the required parameters:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /path/to/neo4j-export.csv \
  -d /path/to/output \
  --bulk-load \
  --bucket-name your-s3-bucket \
  --neptune-endpoint your-cluster.cluster-abc123.region.neptune.amazonaws.com \
  --iam-role-arn arn:aws:iam::account:role/YourNeptuneRole \
  [additional options]
```

#### Required Parameters (when --bulk-load is used)

- `--bucket-name`: S3 bucket name where CSV files will be uploaded
- `--neptune-endpoint`: Neptune cluster endpoint URL
- `--iam-role-arn`: IAM role ARN with Neptune bulk load permissions

#### Optional Parameters

- `--s3-prefix`: S3 prefix for uploaded file (default: neptune)
- `--monitor`: Monitor Neptune bulk load progress until completion (default: true)
- `--parallelism`: Parallelism level for Neptune bulk loading (default: MEDIUM)

#### What happens during bulk load

1. **Convert**: Neo4j CSV is converted to Gremlin load data format
2. **Upload**: Converted CSV files are uploaded to S3
3. **Load**: Neptune bulk load job is initiated
4. **Monitor**: (Optional) Progress is monitored until completion or timeout

#### Example Output

```
Vertices: 174
Edges   : 255
Output  : /tmp/output/1751656971039
/tmp/output/1751656971039

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

### Bulk Load into Neptune (Manual)

If you prefer the manual approach or need more control over the process, you can still use the [Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html) directly to load data into Neptune from the converted CSV files.

This approach gives you more flexibility in terms of:
- Custom S3 upload strategies
- Advanced bulk load configurations
- Integration with existing CI/CD pipelines
- Custom monitoring and error handling
