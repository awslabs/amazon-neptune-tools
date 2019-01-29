# Neptune Export

Exports Amazon Neptune property graph data to CSV or JSON, or RDF graph data to Turtle.

## Usage

  - [`export-pg`](docs/export-pg.md)
  - [`create-pg-config`](docs/create-pg-config.md)
  - [`export-pg-from-config`](docs/export-pg-from-config.md)
  - [`export-pg-from-queries`](docs/export-pg-from-queries.md)
  - [`export-rdf`](docs/export-rdf.md)

### Property Graph

  - [Exporting to the Bulk Loader CSV Format](#exporting-to-the-bulk-loader-csv-format)
  - [Exporting the Results of User-Supplied Queries](#exporting-the-results-of-user-supplied-queries)
  
### RDF Graph

  - [Exporting an RDF Graph](#exporting-an-rdf-graph)
  
### IAM DB authentication

_neptune-export_ supports exporting from databases that have [IAM database authentication](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth.html) enabled. Supply the `--use-iam-auth` option with each command. Remember to set the **SERVICE_REGION** environment variable – e.g. `export SERVICE_REGION=us-east-1`.

_neptune-export_ also supports connecting through a load balancer to a Neptune database with IAM DB authetication enabled. However, this feature is only currently supported for property graphs, with support for RDF graphs coming soon.

If you are connecting through a load balancer, and have IAM DB authentication enabled, you must also supply either an `--nlb-host-header` option (if using a network load balancer) or an `--alb-host-header` option (if using an application load balancer). Use the `--endpoint` and `--port` options to specify the _load balancer_ endpoint and port, and the `--nlb-host-header` or `--alb-host-header` option to specify the Neptune endpoint and port in the form `<NEPTUNE_DNS:PORT>`. For example:

`--nlb-host-header neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com:8182`

For details on using a load balancer with a database with IAM DB authentication enabled, see [Connecting to Amazon Neptune from Clients Outside the Neptune VPC](https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer). 
   
## Building neptune-export

`mvn clean install`
 
## Exporting to the Bulk Loader CSV Format

When exporting to the [CSV format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html) used by the [Amazon Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html), _neptune-export_ generates CSV files based on metadata derived from scanning your graph. This metadata is persisted in a JSON file. There are three ways in which you can use the tool to generate bulk load files:

 - [`export-pg`](docs/export-pg.md) – This command makes two passes over your data: the first to generate the metadata, the second to create the data files. By scanning all nodes and edges in the first pass, the tool captures the superset of properties for each label, identifies the broadest datatype for each property, and identifies any properties for which at least one vertex or edge has multiple values. If exporting to CSV, these latter properties are exported to CSV as array types. If exporting to JSON, these property values are exported as array nodes.
 - [`create-pg-config`](docs/create-pg-config.md) – This command makes a single pass over your data to generate the metadata config file.
 - [`export-pg-from-config`](docs/export-pg-from-config.md) – This command makes a single pass over your data to create the CSV or JSON files. It uses a preexisting metadata config file.
 
### Generating metadata

[`export-pg`](docs/export-pg.md) and [`create-pg-config`](docs/create-pg-config.md) both generate metadata JSON files describing the properties associated with each node and edge label. By default, these commands will scan the entire database. For large datasets, this can take a long time. 

Both commands also allow you to sample a range of nodes and edges in order to create this metadata. If you are confident that sampling your data will yield the same metadata as scanning the entire dataset, specify the `--sample` option with these commands. If, however, you have reason to believe the same property on different nodes or edges could yield different datatypes, or different cardinalities, or that nodes or edges with the same labels could contain different sets of properties, you should consider retaining the default behaviour of a full scan.

### Label filters

All three commands allow you to supply vertex and edge label filters. 

 - If you supply label filters to the [`export-pg`](docs/export-pg.md) command, the metadata file and the exported data files will contain data only for the labels specified in the filters.
 - If you supply label filters to the [`create-pg-config`](docs/create-pg-config.md) command, the metadata file will contain data only for the labels specified in the filters.
 - If you supply label filters to the [`export-pg-from-config`](docs/export-pg-from-config.md) command, the exported data files will contain data for the intersection of labels in the config file and the labels specified in the command filters.
 
### Parallel export

The [`export-pg`](docs/export-pg.md) and [`export-pg-from-config`](docs/export-pg-from-config.md) commands support parallel export. You can supply a concurrency level, which determines the number of client threads used to perform the parallel export, and, optionally, a range or batch size, which determines how many nodes or edges will be queried by each thread at a time. If you specify a concurrency level, but don't supply a range, the tool will calculate a range such that each thread queries _(1/concurrency level) * number of nodes/edges_ nodes or edges.

If using parallel export, we recommend setting the concurrency level to the number of vCPUs on your Neptune instance.

You can load balance requests across multiple instances in your cluster (or even multiple clusters) by supplying multiple `--endpoint` options.

### Long-running queries

_neptune-export_ uses long-running queries to generate the metadata and the data files. You may need to increase the `neptune_query_timeout` [DB parameter](https://docs.aws.amazon.com/neptune/latest/userguide/parameters.html) in order to run the tool against large datasets.

For large datasets, we recommend running this tool against a standalone database instance that has been restored from a snapshot of your database.

## Exporting the Results of User-Supplied Queries

_neptune-export_'s [`export-pg-from-queries`](docs/export-pg-from-queries.md) command allows you to supply groups of Gremlin queries and export the results to CSV or JSON.

Every user-supplied query should return a resultset whose every result comprises a Map. Typically, these are queries that return a `valueMap()` or a projection created using `project().by().by()...`.

Queries are grouped into _named groups_. All the queries in a named group should return the same columns. Named groups allow you to 'shard' large queries and execute them in parallel (using the `--concurrency` option). The resulting CSV or JSON files will be written to a directory named after the group.

You can supply multiple named groups using multiple `--queries` options. Each group comprises a name, an equals sign, and then a semi-colon-delimited list of Gremlin queries. Surround the list of queries in double quotes. For example:

`-q person="g.V().hasLabel('Person').range(0,100000).valueMap();g.V().hasLabel('Person').range(100000,-1).valueMap()"`

Alternatively, you can supply a JSON file of queries.

### Parallel execution of queries

If using parallel export, we recommend setting the concurrency level to the number of vCPUs on your Neptune instance. When _neptune-export_ executes named groups of queries in parallel, it simply flattens all the queries into a queue, and spins up a pool of worker threads according to the concurrency level you have specified using `--concurrency`. Worker threads continue to take queries from the queue until the queue is exhausted.

### Batching

Queries whose results contain very large rows can sometimes trigger a `CorruptedFrameException`. If this happens, adjust the batch size (`--batch-size`) to reduce the number of results returned to the client in a batch (the default is 64).

## Exporting an RDF Graph
              
At present _neptune-export_ supports exporting an RDF dataset to Turtle with a single-threaded long-running query.