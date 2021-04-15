# Neptune Export

Exports Amazon Neptune property graph data to CSV or JSON, or RDF graph data to Turtle.

## Usage

  - [`export-pg`](docs/export-pg.md)
  - [`create-pg-config`](docs/create-pg-config.md)
  - [`export-pg-from-config`](docs/export-pg-from-config.md)
  - [`export-pg-from-queries`](docs/export-pg-from-queries.md)
  - [`export-rdf`](docs/export-rdf.md)

### Topics

  - [Neptune-Export service](#neptune-export-service)
  - [Best practices](#best-practices)
  - [Exporting to the Bulk Loader CSV Format](#exporting-to-the-bulk-loader-csv-format)
  - [Exporting the Results of User-Supplied Queries](#exporting-the-results-of-user-supplied-queries)
  - [Exporting an RDF Graph](#exporting-an-rdf-graph)
  - [Building neptune-export](#building-neptune-export)
  - [Security](#security)
  - [Deploying neptune-export as an AWS Lambda Function](#deploying-neptune-export-as-an-aws-lambda-function)
  
## Neptune-Export service

You can now deploy _neptune-export_ as a service inside your Neptune VPC. Use [these CloudFormation templates](https://docs.aws.amazon.com/neptune/latest/userguide/machine-learning-data-export-service.html) to install the Neptune-Export service.
 
## Best practices

### Export from a cloned cluster

_neptune-export_ cannot guarantee the consistency of exported data if you export from a Neptune cluster whose data is changing while the export is taking place. Therefore, we recommend exporting from a clone of your cluster. This ensures the export takes place against a static version of the data at the point in time the database was cloned. Further, exporting from a clone ensures the export doesn’t impact the query performance of the original cluster.

_neptune-export_ makes it easy to export from a clone. Simply supply a `--clone-cluster` option with the command. You can also use the `--clone-cluster-replica-count` option to specify the number of read replicas to be added to the cloned cluster, and the `--clone-cluster-instance-type` parameter to tell _neptune-export_ which instance type – e.g. `db.r5.2xlarge` –  to use for each instance in the cloned cluster (by default, _neptune-export_ will use the same instance type as the primary in the original cluster.)

If you clone your cluster using the `--clone-cluster` option, _neptune-export_ will ignore any `--concurrency` option supplied in the params, and will instead work out a concurrency setting based on the number of instances in the cloned cluster and their instance types.

If you use the cluster cloning features of _neptune-export_, you must ensure the AWS Identity and Access Management identity with which the process runs can perform the following actions:

  - rds:AddTagsToResource
  - rds:DescribeDBClusters
  - rds:DescribeDBInstances
  - rds:ListTagsForResource
  - rds:DescribeDBClusterParameters
  - rds:DescribeDBParameters
  - rds:ModifyDBParameterGroup
  - rds:ModifyDBClusterParameterGroup
  - rds:RestoreDBClusterToPointInTime
  - rds:DeleteDBInstance
  - rds:DeleteDBClusterParameterGroup
  - rds:DeleteDBParameterGroup
  - rds:DeleteDBCluster
  - rds:CreateDBInstance
  - rds:CreateDBClusterParameterGroup
  - rds:CreateDBParameterGroup


### Use a config file

Use the `export-pg-from-config` command in preference to `export-pg` when exporting property graphs from Neptune. The `export-pg` command makes two passes over your data: the first to generate metadata, the second to create the data files. This first pass takes place on a single thread, and for very large datasets can take many hours – often much longer than the export itself.

The preferred approach is to generate the metadata once using `create-pg-config`, store the config file in S3, and then refer to it from `export-pg-from-config` using the `--config-file` option.

### Supply approximate node and edge counts

When performing a parallel export (`--concurrency` is larger than one), _neptune-export_ must first query your database to determine the number of nodes and edges to be exported. These numbers are then used to calculate ranges for each query in a set of parallel queries. Counting the nodes and edges in a large dataset can take many minutes.

_neptune-export_ now includes `--approx-node-count` and `--approx-edge-count` options that allow you to supply estimates for the number of nodes and edges you expect to export. By specifying approximate counts you can reduce the export time, because _neptune-export_ will no longer have to query the database to count the nodes and edges.

The numbers you supply need only be approximate – it doesn’t matter if you’re within ten percent of the real counts. One way of calculating these numbers is to use the counts from a previous export, adjusted based on the approximate number of additions and deletions that have taken place in the interim. 

## Exporting to the Bulk Loader CSV Format

When exporting to the [CSV format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html) used by the [Amazon Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html), _neptune-export_ generates CSV files based on a schema derived from scanning your graph. This schema is persisted in a JSON file. There are three ways in which you can use the tool to generate bulk load files:

 - [`export-pg`](docs/export-pg.md) – This command makes two passes over your data: the first to generate the schema, the second to create the data files. By scanning all nodes and edges in the first pass, the tool captures the superset of properties for each label, identifies the datatype for each property, and identifies any properties for which at least one vertex or edge has multiple values. If exporting to CSV, these latter properties are exported to CSV as array types. If exporting to JSON, these property values are exported as array nodes.
 - [`create-pg-config`](docs/create-pg-config.md) – This command makes a single pass over your data to generate the schema config file.
 - [`export-pg-from-config`](docs/export-pg-from-config.md) – This command makes a single pass over your data to create the CSV or JSON files. It uses a preexisting schema config file.
 
### Generating schema

[`export-pg`](docs/export-pg.md) and [`create-pg-config`](docs/create-pg-config.md) both generate schema JSON files describing the properties associated with each node and edge label. By default, these commands will scan the entire database. For large datasets, this can take a long time. 

Both commands also allow you to sample a range of nodes and edges in order to create this schema. If you are confident that sampling your data will yield the same schema as scanning the entire dataset, specify the `--sample` option with these commands. If, however, you have reason to believe the same property on different nodes or edges could yield different datatypes, or different cardinalities, or that nodes or edges with the same labels could contain different sets of properties, you should consider retaining the default behaviour of a full scan.

Once you have generated a schema file, either with `export-pg` or `create-pg-config`, you can reuse it for subsequent exports in `export-pg-from-config`. You can also modify the file to restrict the labels and properties that will be exported.

### Label filters

All three commands allow you to supply vertex and edge label filters. 

 - If you supply label filters to the [`export-pg`](docs/export-pg.md) command, the schema file and the exported data files will contain data only for the labels specified in the filters.
 - If you supply label filters to the [`create-pg-config`](docs/create-pg-config.md) command, the schema file will contain data only for the labels specified in the filters.
 - If you supply label filters to the [`export-pg-from-config`](docs/export-pg-from-config.md) command, the exported data files will contain data for the intersection of labels in the config file and the labels specified in the command filters.
 
### Token-only export

For some offline use cases you may want to export only the structural data in the graph: that is, just the labels and IDs of vertices and edges. `export-pg` allows you to specify a `--tokens-only` option with the value `nodes`, `edges` or `both`. A token-only export does not generate a schema, nor does it export any property data: for vertices it simply exports `~id` and `~label`; for edges, it exports `~id`, `~from`, `~to` and `~label`. You can still use label filters to determine exactly which vertices and edges will be exported.
 
### Parallel export

The [`export-pg`](docs/export-pg.md) and [`export-pg-from-config`](docs/export-pg-from-config.md) commands support parallel export. You can supply a concurrency level, which determines the number of client threads used to perform the parallel export, and, optionally, a range or batch size, which determines how many nodes or edges will be queried by each thread at a time. If you specify a concurrency level, but don't supply a range, the tool will calculate a range such that each thread queries _(1/concurrency level) * number of nodes/edges_ nodes or edges.

If using parallel export, we recommend setting the concurrency level to the number of vCPUs on your Neptune instance.

You can load balance requests across multiple instances in your cluster (or even multiple clusters) by supplying multiple `--endpoint` options.

### Long-running queries

_neptune-export_ uses long-running queries to generate the schema and the data files. You may need to increase the `neptune_query_timeout` [DB parameter](https://docs.aws.amazon.com/neptune/latest/userguide/parameters.html) in order to run the tool against large datasets.

For large datasets, we recommend running this tool against a standalone database instance that has been restored from a snapshot of your database.

### Serializer

The latest version of _neptune-export_ uses the [GraphBinary](http://tinkerpop.apache.org/docs/3.4.0/upgrade/#_graphbinary) serialization format introduced in Gremlin 3.4.x. Previous versions of _neptune-export_ used Gryo. To revert to using Gryo, supply `--serializer GRYO_V3D0`.

## Exporting the Results of User-Supplied Queries

_neptune-export_'s [`export-pg-from-queries`](docs/export-pg-from-queries.md) command allows you to supply groups of Gremlin queries and export the results to CSV or JSON.

Every user-supplied query should return a resultset whose every result comprises a Map. Typically, these are queries that return a `valueMap()` or a projection created using `project().by().by()...`.

Queries are grouped into _named groups_. All the queries in a named group should return the same columns. Named groups allow you to 'shard' large queries and execute them in parallel (using the `--concurrency` option). The resulting CSV or JSON files will be written to a directory named after the group.

If there is a possibility that individual rows in a query's resultset will contain different keys, use the `--two-pass-analysis` flag to force _neptune-export_ to determine the superset of keys or column headers for the query.

You can supply multiple named groups using multiple `--queries` options. Each group comprises a name, an equals sign, and then a semi-colon-delimited list of Gremlin queries. Surround the list of queries in double quotes. For example:

`-q person="g.V().hasLabel('Person').range(0,100000).valueMap();g.V().hasLabel('Person').range(100000,-1).valueMap()"`

Alternatively, you can supply a JSON file of queries.

### Parallel execution of queries

If using parallel export, we recommend setting the concurrency level to the number of vCPUs on your Neptune instance. When _neptune-export_ executes named groups of queries in parallel, it simply flattens all the queries into a queue, and spins up a pool of worker threads according to the concurrency level you have specified using `--concurrency`. Worker threads continue to take queries from the queue until the queue is exhausted.

### Batching

Queries whose results contain very large rows can sometimes trigger a `CorruptedFrameException`. If this happens, you can either adjust the batch size (`--batch-size`) to reduce the number of results returned to the client in a batch (the default is 64), or increase the frame size (`--max-content-length`, default value 65536).

## Exporting an RDF Graph
              
At present _neptune-export_ supports exporting an RDF dataset to Turtle with a single-threaded long-running query.

## Security
  
### Encryption in transit

By default, _neptune-export_ connects to your database using SSL. If your target does not support SSL connections, use the `--disable-ssl` flag.

(SSL used to be an opt-in feature for _neptune-export_, with a `--use-ssl` option for turning SSL on. This behaviour has now changed: SSL is on by default, but can be turned off using `--disable-ssl`. The `--use-ssl` option now no longer has any effect.)

If you are using a load balancer or a proxy server (such as HAProxy), you must [use SSL termination and have your own SSL certificate on the proxy server](https://docs.aws.amazon.com/neptune/latest/userguide/security-ssl.html).

### IAM DB authentication

_neptune-export_ supports exporting from databases that have [IAM database authentication](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth.html) enabled. Supply the `--use-iam-auth` option with each command. Remember to set the **SERVICE_REGION** environment variable – e.g. `export SERVICE_REGION=us-east-1`.

_neptune-export_ also supports connecting through a load balancer to a Neptune database with IAM DB authetication enabled. However, this feature is only currently supported for property graphs, with support for RDF graphs coming soon.

If you are connecting through a load balancer, and have IAM DB authentication enabled, you must also supply either an `--nlb-endpoint` option (if using a network load balancer) or an `--alb-endpoint` option (if using an application load balancer), and an `--lb-port`.

For details on using a load balancer with a database with IAM DB authentication enabled, see [Connecting to Amazon Neptune from Clients Outside the Neptune VPC](https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer). 
   
## Building neptune-export

To build the jar, run:

`mvn clean install`

## Deploying neptune-export as an AWS Lambda Function

The _neptune-export_ jar can be deployed as an AWS Lambda function. To access Neptune, you will either have to [configure the function to access resources inside your VPC](https://docs.aws.amazon.com/lambda/latest/dg/vpc.html), or [expose the Neptune endpoints via a load balancer](https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer).

Be mindful of the [AWS Lambda limits](https://docs.aws.amazon.com/lambda/latest/dg/limits.html), particularly with regard to function timeouts (max 15 minutes) and _/tmp_ directory storage (512 MB). Large exports can easily exceed these limits.

When deployed as a Lambda function, _neptune-export_ will automatically copy the export files to an S3 bucket of your choosing. Optionally, it can also write a completion file to a separate S3 location (useful for triggering additional Lambda functions). You must configure your function with an IAM role that has write access to these S3 locations.

The Lambda function expects a number of parameters, which you can supply either as [environment variables](https://docs.aws.amazon.com/lambda/latest/dg/env_variables.html) or via a JSON input parameter. Fields in the JSON input parameter override any environment variables you have set up.

| Environment Variable | JSON Field | Description ||
| ---- | ---- | ---- | ---- |
| `COMMAND` | `command` | _neptune-export_ command and command-line options: e.g. `export-pg -e <neptune_endpoint>` | Mandatory |
| `OUTPUT_S3_PATH` | `outputS3Path` | S3 location to which exported files will be written | Mandatory |
| `CONFIG_FILE_S3_PATH` | `configFileS3Path` | S3 location of a JSON config file to be used when exporting a property graph from a config file | Optional |
| `COMPLETION_FILE_S3_PATH` | `completionFileS3Path` | S3 location to which a completion file should be written once all export files have been copied to S3 | Optional |