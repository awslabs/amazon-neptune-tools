## Amazon Neptune Tools

Utilities to enable loading data and building graph applications with Amazon Neptune.

### Examples

You may also be interested in the [Neptune Samples github repository](https://github.com/aws-samples/amazon-neptune-samples), which includes samples and example code.

### GraphML 2 CSV
This is a [utility](graphml2csv/README.md) to convert graphml files into the Neptune CSV format.

### Neptune Gremlin Client

[neptune-gremlin-client](neptune-gremlin-client/) is a Java Gremlin client for Amazon Neptune that allows you to change the endpoints used by the client as it is running. Includes an agent that can query the Amazon Neptune API for cluster details, and update the client on a periodic basis. You can supply your own custom endpoint selectors to configure the client for a subset of instances in your cluster based on tags, instance types, instance IDs, AZs, etc.

The client also provides support for IAM database authentication, and for connecting to Neptune via a network or application load balancer.

![Rotating Endpoints](neptune-gremlin-client/rotating-endpoints.png)

### Neptune Export
Exports Amazon Neptune data to CSV for Property Graph or Turtle for RDF graphs.

You can use [neptune-export](neptune-export/) to export an Amazon Neptune database to the bulk load CSV format used by the Amazon Neptune bulk loader for Property Graph or Turtle for RDF graphs. Alternatively, you can supply your own queries to neptune-export and unload the results to CSV or Turtle.

### Export Neptune to Elasticsearch
Backfills Elasticsearch with data from an existing Amazon Neptune database.

The [Neptune Full-text Search](https://docs.aws.amazon.com/neptune/latest/userguide/full-text-search-cfn-create.html) CloudFormation templates provide a mechanism for indexing all _new_ data that is added to an Amazon Neptune database in Elasticsearch. However, there are situations in which you may want to index _existing_ data in a Neptune database prior to enabling the full-text search integration.

You can use this [export Neptune to Elasticsearch solution](export-neptune-to-elasticsearch/) to index existing data in an Amazon Neptune database in Elasticsearch.

### Neo4j to Neptune
A [command-line utility](neo4j-to-neptune/readme.md) for migrating data to Neptune from Neo4j.

### Glue Neptune

[glue-neptune](glue-neptune/) is a Python library for AWS Glue that helps writing data to Amazon Neptune from Glue jobs. With glue-neptune you can:
* Get Neptune connection information from the Glue Data Catalog
* Create label and node and edge ID columns in DynamicFrames, named in accordance with the Neptune CSV bulk load format for property graphs
* Write from DynamicFrames directly to Neptune

### Neptune CSV to RDF

If you're interested in converting Neptune's CSV format to RDF, see [amazon-neptune-csv-to-rdf-converter](https://github.com/aws/amazon-neptune-csv-to-rdf-converter).

## License

This library is licensed under the Apache 2.0 License. 
