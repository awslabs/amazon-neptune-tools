# neo4j-to-neptune

A command-line utility for migrating data from Neo4j to Neptune.

### Examples

**Conversion only:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /path/to/neo4j-export.csv \
  --dir output \
  --infer-types
```

**Conversion only using configuration file:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --conversion-config /path/to/conversion-config.yaml \
  --dir output \
  --infer-types
```

**Convert and bulk load to Neptune:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /tmp/neo4j-export.csv \
  --dir output \
  --neptune-endpoint my-cluster.cluster-abc123.us-east-2.neptune.amazonaws.com \
  --bucket-name my-neptune-bucket \
  --iam-role-arn arn:aws:iam::123456789012:role/NeptuneLoadFromS3 \
  --infer-types
```

**Convert and bulk load using configuration file:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /tmp/neo4j-export.csv \
  --dir output \
  --bulk-load-config /path/to/bulk-load-config.yaml \
  --infer-types
```

**Convert and bulk load to Neptune:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /tmp/neo4j-export.csv \
  -d output \
  --neptune-endpoint my-cluster.cluster-abc123.us-east-2.neptune.amazonaws.com \
  --bucket-name my-neptune-bucket \
  --iam-role-arn arn:aws:iam::123456789012:role/NeptuneLoadFromS3 \
  --infer-types
```

**Convert and bulk load using configuration file:**
```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /tmp/neo4j-export.csv \
  -d output \
  --bulk-load-config bulk-load-config.yaml \
  --infer-types
```

### Build

```
mvn clean install
```

### Usage

  - [`convert-csv`](docs/convert-csv.md)

## Migration Process

Migration of data from Neo4j to Neptune can be accomplished in a few ways:

### Option 1: Manual Multi-Step

 1. [**Export CSV from Neo4j**](#export-csv-from-neo4j) – Use the APOC export procedures to export data from Neo4j to CSV.
 2. [**Convert CSV**](#convert-csv) – Use the `convert-csv` command-line utility to convert the exported CSV into the Neptune Gremlin bulk load CSV format.
 3. [**Bulk Load into Neptune**](#bulk-load-into-neptune-manual) – Use the Neptune bulk load API to load data into Neptune.

### Option 2: Local Convert then Bulk Load

 1. [**Export CSV from Neo4j**](#export-csv-from-neo4j) – Use the APOC export procedures to export data from Neo4j to CSV.
 2. [**Convert and Bulk Load**](#convert-and-bulk-load) – Use the `convert-csv` command with either `--bulk-load-config` or the required combination of `--bucket-name`, `--neptune-endpoint`, and `--iam-role-arn` to automatically convert the CSV, upload it to S3, and initiate a bulk load into Neptune.

### Option 3: Automatically Stream and Convert then Bulk Load

 1. [**Stream and Convert CSV**](#convert-csv) – Use the `convert-csv` command with either `--dotenv-file` or all of `--uri`, `--username` and `--password` to provide Neo4j credentials and stream the data without manually exporting it.
 2. [**Bulk Load**](#convert-and-bulk-load) - Specify either `--bulk-load-config` or the required combination of `--bucket-name`, `--neptune-endpoint`, and `--iam-role-arn`as the bulk load parameters.

### Export CSV from Neo4j

Use the [`apoc.export.csv.all`](https://neo4j.com/docs/labs/apoc/current/export/csv/) procedure from neo4j's [APOC](https://neo4j.com/docs/labs/apoc/current/) library to export data from Neo4j to CSV.

#### 1. Install APOC

Follow the [instructions](https://neo4j.com/docs/labs/apoc/current/introduction/) for installing the APOC library for either Neo4j Desktop or Neo4j Server.

#### 2. Enable exports

Create or update the `apoc.conf` configuration file to enable exports:

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

Use the [`convert-csv`](docs/convert-csv.md) command-line utility to convert the CSV files exported from Neo4j into the Neptune Gremlin bulk load CSV format.

The utility requires two sets of parameters:

1. **Input method from Neo4j** — Provide one of the following:
  - [Manual] A path to locally exported Neo4j CSV files using the `--input` or `-i` flag.
  - [Stream] A path to a `.env` file using the `--dotenv-file` or `-df` flag. This file must define the variables `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD`, corresponding to the Neo4j URI, username, and password.
  - [Stream] Directly specify the Neo4j URI, username, and password using the flags `--uri` or `-u`, `--username` or `-n`, and `--password` or `-pw`
2. **Output directory** — The path to the local directory where the converted CSV files will be written.

There are also optional parameters that allow you to specify node, relationship multi-valued property policies, turn on data type inferencing, or to use a configuration YAML file for more granular conversion manipulation.

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


#### Configuration Methods

##### Method 1: Using Local Exported Neo4j CSV

To convert a local CSV that was exported from Neo4j, provide the required parameters:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /path/to/neo4j-export.csv \
  --dir /path/to/output \
  [additional options]
```

##### Method 2: Using Stream with Environment files

To stream data from Neo4j to Neptune

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --dotenv-file /path/to/dotenv/file \
  --dir /path/to/output \
  [additional options]
```

##### Method 3: Using Stream with Neo4j credentials files

To stream data from Neo4j to Neptune

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --uri <Neo4j connection uri> \
  --username <Neo4j username> \
  --password <Neo4j password> \
  --dir /path/to/output \
  [additional options]
```

#### Configuration with YAML

The `convert-csv` utility conversion process can be configured through the YAML file for ID transformation, label mapping, and filtering:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /path/to/neo4j-export.csv \
  --dir /path/to/output \
  --conversion-config conversion-config.yaml \
  [additional options]
```

##### Example configuration file
For an example of the conversion config YAML file refer to [`docs/example-conversion-config.yaml`](docs/example-conversion-config.yaml)

##### ID Transformation Templates

Templates can reference original data fields using placeholders:

- **Vertex templates**: `{_id}`, `{_labels}`, `{property_name}`
- **Edge templates**: `{_type}`, `{_start}`, `{_end}`, `{~from}`, `{~to}`, `{property_name}`

##### Examples
- `"{_labels}_{name}_{_id}"` → `"Person_John_123"`
- `"e!{~label}_{~from}_{~to}"` → `"e!KNOWS_Person_123_Person_456"`

The utility uses two-pass processing: first transforming vertices and building ID mappings, then processing edges with correct vertex references. When vertices are skipped, connected edges are automatically skipped to maintain graph consistency.

### Bulk Load into Neptune

Use the [Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html) to load data into Neptune from the converted CSV files.

### Convert and Bulk Load

The `convert-csv` command supports an integrated bulk loading option that automatically uploads converted CSV files to S3 and initiates the Neptune bulk load process. You can configure bulk loading using either CLI parameters or a YAML configuration file.

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

#### Configuration Methods

##### Method 1: Using CLI Parameters

To enable bulk load using CLI parameters, provide the required Neptune configuration:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /path/to/neo4j-export.csv \
  --dir /path/to/output \
  --neptune-endpoint your-cluster.cluster-abc123.region.neptune.amazonaws.com \
  --bucket-name your-s3-bucket \
  --iam-role-arn arn:aws:iam::account:role/YourNeptuneRole \
  [additional options]
```

##### Method 2: Using Configuration File

Create a YAML configuration file with your bulk load settings:

**bulk-load-config.yaml:**
```yaml
# S3 Configuration
bucket-name: "my-neptune-bulk-load-bucket"
s3-prefix: "neptune"

# Neptune Configuration
neptune-endpoint: "my-neptune-cluster.cluster-abc123def456.us-east-1.neptune.amazonaws.com"
neptune-port: "8182"

# IAM Configuration
iam-role-arn: "arn:aws:iam::123456789012:role/NeptuneLoadFromS3Role"

# Performance Settings
parallelism: "OVERSUBSCRIBE"  # Options: LOW, MEDIUM, HIGH, OVERSUBSCRIBE

# Monitoring
monitor: true
```

Then use the configuration file:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /path/to/neo4j-export.csv \
  --dir /path/to/output \
  --bulk-load-config bulk-load-config.yaml \
  [additional options]
```

##### Method 3: Hybrid Approach (Configuration File + CLI Overrides)

You can use a configuration file and override specific parameters via CLI:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  --input /path/to/neo4j-export.csv \
  --dir /path/to/output \
  --bulk-load-config bulk-load-config.yaml \
  --neptune-endpoint different-cluster.cluster-xyz789.us-west-2.neptune.amazonaws.com \
  --parallelism HIGH \
  [additional options]
```

[!IMPORTANT]
**Parameter Precedence:** CLI parameters override configuration file values, which override default values.

#### Required Parameters

The following parameters must be provided either via CLI or configuration file:

- **Neptune endpoint**: `--neptune-endpoint` or `neptune-endpoint` in YAML
- **S3 bucket name**: `--bucket-name` or `bucket-name` in YAML
- **IAM role ARN**: `--iam-role-arn` or `iam-role-arn` in YAML

#### Optional Parameters

- **Neptune port**: `--neptune-port` or `neptune-port` in YAML (default: "8182")
- **S3 prefix**: `--s3-prefix` or `s3-prefix` in YAML
- **Parallelism**: `--parallelism` or `parallelism` in YAML (default: "OVERSUBSCRIBE")
  - Options: `LOW`, `MEDIUM`, `HIGH`, `OVERSUBSCRIBE`
- **Monitor progress**: `--monitor` or `monitor` in YAML (default: false)

#### Early Validation

When bulk load parameters are provided (either via `--bulk-load-config` or `--neptune-endpoint`), the tool validates all bulk load parameters before starting the conversion process. If any required parameters are missing or invalid, the conversion will be aborted with a clear error message indicating which parameters are missing.

#### What happens during bulk load

1. **Validate**: All bulk load parameters are validated before conversion starts
2. **Convert**: Neo4j CSV is converted to Gremlin load data format
3. **Upload**: Converted CSV files are uploaded to S3
4. **Load**: Neptune bulk load job is initiated
5. **Monitor**: (Optional) Progress is monitored until completion or timeout

#### Example Output

```
java -jar target/neo4j-to-neptune.jar convert-csv -d output --uri bolt://localhost:7687 --username neo4j --password password --bulk-load-config bulk-load-config.yaml
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
Sep. 04, 2025 12:14:34 P.M. com.amazonaws.services.neptune.io.Neo4jStreamWriter <init>
INFO: Successfully connected to Neo4j database at: bolt://localhost:7687
Sep. 04, 2025 12:14:34 P.M. com.amazonaws.services.neptune.io.Neo4jStreamWriter streamToFile
INFO: Starting data export to file: /tmp/output/1757013274850/neo4j-stream-data-temp.csv
Sep. 04, 2025 12:14:35 P.M. com.amazonaws.services.neptune.io.Neo4jStreamWriter streamToFile
INFO: Successfully exported 1 records (425 lines) to file: /tmp/output/1757013274850/neo4j-stream-data-temp.csv
Sep. 04, 2025 12:14:35 P.M. com.amazonaws.services.neptune.io.Neo4jStreamWriter close
INFO: Neo4j driver closed successfully
Vertices: 171
Edges   : 253
Output  : output/1757013274850
output/1757013274850

Completed in 0 second(s)
S3 Bucket: my-bucket
S3 Prefix: neptune
AWS Region: us-west-2
IAM Role ARN: arn:aws:iam::123456789100:role/NeptuneReadS3
Neptune Endpoint: db-neptune-1.cluster-xxxxxxxxxxxx.us-west-2.neptune.amazonaws.com
Neptune Port: 8182
Bulk Load Parallelism: OVERSUBSCRIBE
Bulk Load Monitor: true

Uploading Gremlin load data to S3...
Starting sequential upload of files from /tmp/output/1757013274850 to s3://my-bucket/neptune/1757013274850
Uploading file 1 of 2: vertices.csv
Starting upload with compression of /tmp/output/1757013274850/vertices.csv to s3://my-bucket/neptune/1757013274850/vertices.csv.gz
File size: 7.2 KB
Initiating Transfer Manager upload...
Upload with compression completed for /tmp/output/1757013274850/vertices.csv
Successfully uploaded vertices.csv (1/2)
Uploading file 2 of 2: edges.csv
Starting upload with compression of /tmp/output/1757013274850/edges.csv to s3://my-bucket/neptune/1757013274850/edges.csv.gz
File size: 17.6 KB
Initiating Transfer Manager upload...
Upload with compression completed for /tmp/output/1757013274850/edges.csv
Successfully uploaded edges.csv (2/2)
Successfully uploaded all 2 files from /tmp/output/1757013274850
Files uploaded successfully to S3. Files available at: s3://my-bucket/neptune/1757013274850/
Starting Neptune bulk load...
Testing connectivity to Neptune endpoint...
Successfully connected to Neptune. Status: 200 healthy
Neptune bulk load started successfully with load ID: a478c673-xxxx-xxxx-xxxx-77a70a430be8
Monitoring load progress for job: a478c673-xxxx-xxxx-xxxx-77a70a430be8
Neptune bulk load status: LOAD_IN_PROGRESS
Neptune bulk load status: LOAD_IN_PROGRESS
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
