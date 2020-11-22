# csv-gremlin

Convert Amazon Neptune format CSV files into Gremlin steps that can be used to load the data.

### Overview

This folder contains the code defining a `NeptuneCSVReader` class. The class is used to create a tool able to read CSV files that use the Amazon Neptune formatting
rules and generate Gremlin steps from that data.  Those Gremlin steps can then be used to load the data into any TinkerPop compliant graph that allows for user defined Vertex and Edge IDs.

The tool can detect and handle both the vertex and edge CSV file formats. It
recognizes the Neptune type specifiers, such as `age:Int` and `birthday:Date`. If no type mofier is specified in the column header, a type of `String`
is assumed.  It also handles rows containing sparse data such as `,,,`.

The tool also allows you to specify the batch size for vertices and edges. The
default is set to 10 for each currently. Batching allows multiple vertices or
edges, along with their properties, to be added in a single Gremlin query. This is often more efficient than loading them one at a time.

For CSV files containing vertices, the special column headers `~id` and `~label` and their respective values are expected to be present. However, if `~label` is not present a default vertex lable of 'vertex' will be used. For CSV files containing edges all of the following special columns are expected to be present:  `~id`, `~label`, `~from`, `~to` and a value is expected for each in every row.

Gremlin steps that represent the data in the CSV are written to `stdout`.

### Current Limitations

Currently the tool does not support cardinality column headers such as
`age:Int(single)`. Likewise lists of values declared using the `[]` column
header modifier are not supported.

### Running the tool

The code is written in Python and has been tested using version 3.7.6. This tool does not support Python 2. Generating Gremlin steps from a CSV file is as simple as:

```
python csv-gremlin.py my-csvfile.csv
```
Where `my-csvfile.csv` is the name of the file to be processed. There are some command line arguments that can be used to specify the size of the batches used for vertices and for edges. For example to use a batch size of 20 for each you can use the following command.
```
 python csv-gremlin.py  -vb 20 -eb 20 test.csv
```

For columns that contain `Date` values you can choose to have the values used as-is and output in the form `datetime(<the original date string>)` in the Gremlin query or converted to the Java Date form of `new Date(<original date converted to an epoch offset>)`. The `-use_java_dates` argument should be specified if Java format date conversions are required. Further, if dates in the CSV file do not include Time Zone information you can choose to have them treated as local time or as UTC. To specify UTC time as the default use the `assume_utc` argument.

By default all rows of  the CSV file will be processed. To process less rows you can use the `rows` argument to specify a maximum number of rows to process.

The help can always be displayed using the `-h` or `--help` command line arguments.
```
$ python csv-gremlin.py -h
usage: csv-gremlin.py [-h] [-v] [-vb VB] [-eb EB] [-java_dates] [-assume_utc]
                      [-rows ROWS]
                      csvfile

positional arguments:
  csvfile        The name of the CSV file to process

optional arguments:
  -h, --help     show this help message and exit
  -v, --version  Display version information
  -vb VB         Set the vertex batch size to use (default 10)
  -eb EB         Set the edge batch size to use (default 10)
  -java_dates    Use Java style "new Date()" instead of "datetime()"
  -assume_utc    If date fields do not contain timezone information, assume
                 they are in UTC. By default local time is assumed otherwise.
                 This option only applies if java_dates is also specified.
  -rows ROWS     Specify the maximum number of rows to process. By default the
                 whole file is processed


  ```
