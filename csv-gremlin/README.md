# csv-gremlin

Convert Amazon Neptune format CSV files into Gremlin steps that can be used to load the data.

The Amazon Neptune CSV format is defined at https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html

### Overview

This folder contains the code defining a `NeptuneCSVReader` class and an application that allows the class to be invoked along with various optional arguments. The tool can be used to read CSV files that use the Amazon Neptune formatting rules and generate Gremlin steps from that data.  Those Gremlin steps can then be used to load the data into any TinkerPop compliant graph that allows for user defined Vertex and Edge IDs. Errors in the CSV files can also be detected.

The tool can detect and handle both the vertex and edge CSV file formats. This is done by inspecting the CSV file header row. The tool also
recognizes the Neptune type specifiers, such as `age:Int` and `birthday:Date`. If no type modifier is specified in the column header, a type of `String`
is assumed as in `firstName`.  Rows containing sparse data such as `,,,` are handled appropriately.

Options are provided that allow you to specify the batch size for vertices and edges. The default for each is 10. Batching allows multiple vertices or
edges, along with their properties, to be added in a single Gremlin query. This is often more efficient than loading them one at a time.

For CSV files containing vertices, the special column headers `~id` and `~label` and their respective values are expected to be present. However, if `~label` is not present a default vertex lable of 'vertex' will be used. For CSV files containing edges all of the following special columns are expected to be present:  `~id`, `~label`, `~from`, `~to` and a value is expected for each in every row. In all cases, if `~id` is not present in the header row, an error will be generated.

Gremlin steps that represent the data in the CSV are written to `stdout` errors are written to `stderr`. 

### Current Limitations

Currently the tool does not support the special cardinality column headers such as `age:Int(single)`. However, lists of values declared using the `[]` column
header modifier are supported and will generate `property` steps that use the `set` cardinality keyword. So you can specifiy a header such as `score:Int[]` and
in the respective row/column position specify a list of values delimited by semicolons such as `1;2;3;4;5`. For CSV files that define vertices, ID values may appear on more than one row and will be handled appropriately. See [Repeating IDs](#rid). As the cardinality column headers are not recognized, properties with Set cardinality will be created whenever a vertex ID appears more than once.

### Running the tool

The code is written in Python and has been tested using version 3.7.6. This tool does not support Python 2. Generating Gremlin steps from a CSV file is as simple as:

```
python csv-gremlin.py my-csvfile.csv
```
Where `my-csvfile.csv` is the name of the file to be processed. There are some command line arguments that can be used to specify the size of the batches used for vertices and for edges. For example to use a batch size of 20 for a file of vertices you can use the following command.
```
 python csv-gremlin.py  -vb 20  vertices.csv
```

By default all rows of  the CSV file will be processed. To process less rows you can use the `rows` argument to specify a maximum number of rows to use. The tool uses the CSV header to try and identify if the file contains vertex data or edge data. If either of `~from` or `~to` is present the file will be treated as if it contains edge data. In all other cases the tool will assume the CSV file contains vertex data. Note that if one but not both of `~from` and `~to` is present the tool will generate an error as both are required for files of edge data.

### Date processing

For columns that contain `Date` values you can choose to have the values used as-is and output in the form `datetime(<the original date string>)` or converted to the Java Date form of `new Date(<original date converted to an epoch offset>)`. The `-use_java_dates` argument should be specified if Java format date conversions are required. Further, if dates in the CSV file do not include Time Zone information you can choose to have them treated as local time or as UTC. To specify UTC time as the default use the `assume_utc` argument. 

Dates will also be checked for ISO 8601 conformance if `-java_dates` is used. The default behavior is to just take the value present in any `Date` column and copy it to the output. To validate `Date` columns you can run the tool with `-java_dates` enabled to check for any errors and then re-run  it without the option specified if you want to generate `datetime()` style dates in the Gremlin output. 

<a name="rid"></a>
### Repeating IDs

The Neptune bulk loader allows the same ID to appear on multiple rows of a vertex CSV file. In such cases the first time the ID appears the vertex will be created and for subsequent rows the properties will be updated appropriately rather than a new vertex created. This technique is sometimes used to build up a property containing a set of values or to add new properties to a vertex using multiple CSV rows. The `csv-gremlin` tool supports this pattern also. 

For CSV files that define vertices, if the same ID appears on more than one row, Gremlin `property` steps will be created with Set cardinality each time an ID is repeatedly  seen. For edge files, where Set cardinality is not supported, if an ID appears more than once, the tool will generate an error. 

### Error detection

By default the tool will exit as soon as it finds any error in a CSV file. You can override this and have the tool attempt to find all
errors using the `-all_errors` argument. This allows `csv-gremlin` to be used as a Neptune CSV validator as well as a Gremlin generator. Many of
the most common errors can  be detected. These include:

- Missing required headers or required values (such as for ~id)
- Columns without a value for a defined header
- Rows containing extra columns not declared in the header
- Invalid dates
- Invalid numeric values
- Edge files that attempt to define sets of values using `[]`
- Edge files with repeating ID values on multiple rows

There are a few cases where the `-all_errors` argument is ignored. They are all related to issues with the header row of a CSV file. If an edge file includes any set identifiers `[]` in the header row, processing will stop immediately. Likewise processing is aborted if the `~id` column is missing in the header of any CSV file. Finally processing will stop if any of `~label`, `~from`, `~to` are missing in what appears to be a file of edge definitions.

There are likely to be other errors that the tool currently does not detect. Please open an issue if you encounter any of these.

To find as many errors as possible in the file `my-file.csv`, including checking dates for validity, use the following arguments:
```
  python csv-gremlin -java_dates -all_errors my-file.csv
```
To only see error messages and prevent any Gremlin steps from being generated the `-silent` argument can be used. This is useful if you want to check a large CSV file for errors before starting to generate Gremlin queries.
```
  python csv-gremlin -java_dates -all_errors -silent my-file.csv
```
### Processing summary
At the end of processing a summary report will be printed. It is written to `stderr` so that if the Gremlin steps are being redirected to a file, the summary report will not get appended to that file. To turn off the summary report the `-no_summary` argument can be used.

```
$ python csv-gremlin.py test-files/vertices-with-repeat-ids.csv -silent -all_errors

Processing Summary
------------------
Rows=13, IDs=5, Duplicate IDs=7, Vertices=5, Edges=0, Properties=17, Errors=0
```

### Getting help

The help can always be displayed using the `-h` or `--help` command line arguments.
```
$ python3 csv-gremlin.py -h
usage: csv-gremlin.py [-h] [-v] [-vb VB] [-eb EB] [-java_dates] [-assume_utc]
                      [-rows ROWS] [-all_errors] [-silent] [-no_summary]
                      [-double_suffix] [-skip_spaces] [-escape_dollar]
                      csvfile

positional arguments:
  csvfile         The name of the CSV file to process

optional arguments:
  -h, --help      show this help message and exit
  -v, --version   Display version information
  -vb VB          Set the vertex batch size to use (default 10)
  -eb EB          Set the edge batch size to use (default 10)
  -java_dates     Use Java style "new Date()" instead of "datetime()". This
                  option can also be used to force date validation.
  -assume_utc     If date fields do not contain timezone information, assume
                  they are in UTC. By default local time is assumed otherwise.
                  This option only applies if java_dates is also specified.
  -rows ROWS      Specify the maximum number of rows to process. By default
                  the whole file is processed
  -all_errors     Show all errors. By default processing stops after any error
                  in the CSV is encountered.
  -silent         Enable silent mode. Only errors are reported. No Gremlin is
                  generated.
  -no_summary     Do not show a summary report after processing.
  -double_suffix  Suffix all floats and doubles with a "d" such as 12.34d.
                  This is helpful when using the Gremlin Console or Groovy
                  scripts as it will prevent floats and doubles automatically
                  being created as BigDecimal objects.
  -skip_spaces    Skip any leading spaces in each column. By defaut this
                  setting is False and any leading spaces will be considered
                  part of the column header or data value. This setting does
                  not apply to values enclosed in quotes such as " abcd".
  -escape_dollar  For any dollar signs found convert them to an escaped form
                  \$. This is needed if you are going to load the generated
                  Gremlin using a Groovy processor such as used by the Gremlin
                  Console. In Groovy strings, the $ sign is used for
                  interpolation

  ```
