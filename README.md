## Amazon Neptune Tools

Utilities to enable loading data and building graph applications with Amazon Neptune.

### Examples

You may also be interested in the [Neptune Samples github repository](https://github.com/aws-samples/amazon-neptune-samples), which includes samples and example code.

### GraphML 2 CSV
This is a [utility](graphml2csv/README.md) to convert graphml files into the Neptune CSV format.

### Neptune Export
Exports Amazon Neptune data to CSV for Property Graph or Turtle for RDF graphs.

You can use [neptune-export](neptune-export/) to export an Amazon Neptune database to the bulk load CSV format used by the Amazon Neptune bulk loader for Property Graph or Turtle for RDF graphs. Alternatively, you can supply your own queries to neptune-export and unload the results to CSV or Turtle.

### Glue Neptune

[glue-neptune](glue-neptune/) is a Python library for AWS Glue that helps writing data to Amazon Neptune from Glue jobs. With glue-neptune you can:
* Get Neptune connection information from the Glue Data Catalog
* Create label and node and edge ID columns in DynamicFrames, named in accordance with the Neptune CSV bulk load format for property graphs
* Write from DynamicFrames directly to Neptune

## License

This library is licensed under the Apache 2.0 License. 
