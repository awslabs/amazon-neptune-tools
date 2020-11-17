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

The code is written in Python. Generating Gremlin steps from a CSV file is as simple as:

```
python csv-gremlin.py my-csvfile.csv
```
Where `my-csvfile.csv` is the name of the file to be processed. There are currently no command line options other than the file name.
