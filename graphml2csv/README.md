# GraphML 2 Neptune CSV

This Python script provides a utility to convert GraphML files into the CSV format that is used by Amazon Neptune for [Bulk Loading](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html). This script is compatible with Python2 and Python3.

## Usage

```
Usage: graphml2csv.py [options]

Copyright 2018 Amazon.com, Inc. or its affiliates.
Licensed under the Apache License 2.0 http://aws.amazon.com/apache2.0/

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -i FILE, --in=FILE    set input path [default: none]
  -d DELIMITER, --delimiter=DELIMITER
                        Set the output file delimiter [default: ,]
  -e ENCODING, --encoding=ENCODING
                        Set the input file encoding [default: utf-8]

A utility python script to convert GraphML files into the Amazon Neptune CSV
format for bulk ingestion. See
https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html.
```

## Example Using the Tinkerpop modern graph.

Download the tinkerpop-modern.xml graphml file.

```
$ curl https://raw.githubusercontent.com/apache/tinkerpop/master/data/tinkerpop-modern.xml -o tinkerpop-modern.xml
```

Execute the Python script to produce two csv files: nodes and edges.

```
$ ./graphml2csv.py -i tinkerpop-modern.xml 
infile = tinkerpop-modern.xml
Processing tinkerpop-modern.xml
Wrote 6 nodes and 18 attributes to tinkerpop-modern-nodes.csv.
Wrote 6 edges and 12 attributes to tinkerpop-modern-edges.csv.
```

Upload the csv files into your S3 bucket and [bulk load](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html) into Neptune.
