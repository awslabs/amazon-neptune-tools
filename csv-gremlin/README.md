# csv-gremlin

Convert Amazon Neptune format CSV files into Gremlin steps that can be used to load the data.

### Overview

This folder contains the code defining a `NeptuneCSVReader` class. The class is used to create a tool able to read CSV files that use the Amazon Neptune formatting
rules and generate Gremlin steps from that data.  Those Gremlin steps can then be used to load the data into any TinkerPop compliant graph that allows for user defined Vertex and Edge IDs.

The tool can detect and handle both the vertex and edge CSV file formats. It
recognizes the Neptune type specifiers, such as `age:Int` and defaults to String
if none is provided in a column header.  It also handles rows containing sparse data such as `,,,`.

The tool also allows you to specify the batch size for vertices and edges. The
default is set to 10 for each currently. Batching allows multiple vertices or
edges, along with their properties, to be added in a single Gremlin query. This is often more efficient than loading them one at a time.

Gremlin steps that represent the data in the CSV are written to `stdout`.

### Current Limitations

Currently the tool does not support cardinality column headers such as
`age:Int(single)`. Likewise lists of values declared using the `[]` column
header modifier are not supported.

In this initial version none of the special column headers are allowed to
be omitted. Those being (`~id`, `~label`, `~from`, `~to`).

### Running the tool

The code is written in Python and has been tested using version 3.7.6. This tool does not support Python 2. Generating Gremlin steps from a CSV file is as simple as:

```
python csv-gremlin.py my-csvfile.csv
```
Where `my-csvfile.csv` is the name of the file to be processed. There are some command line arguments that can be used to specify the size of the batches used for vertices and for edges. For example to use a batch size of 20 for each you can use the following command.
```
 python csv-gremlin.py  -vb 20 -eb 20 test.csv
```
The help can always be displayed using the `-h` or `--help` command line arguments.
```
$ python csv-gremlin.py  --help
usage: csv-gremlin.py [-h] [-v] [-vb VB] [-eb EB] csvfile

positional arguments:
  csvfile        the name of the CSV file to process

optional arguments:
  -h, --help     show this help message and exit
  -v, --version  display version information
  -vb VB         set the vertex batch size to use (default 10)
  -eb EB         set the edge batch size to use (default 10)
  ```
