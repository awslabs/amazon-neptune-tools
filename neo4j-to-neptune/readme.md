# neo4j-to-neptune

A command-line utility for migrating data from Neo4j to Neptune.

### Examples

```
java -jar neo4j-to-neptune.jar convert-csv -i /tmp/neo4j-export.csv -d output --infer-types
```

### Build

```
mvn clean install
```

### Usage

  - [`convert-csv`](docs/convert-csv.md)
 
## Migration Process

Migration of data from Neo4j to Neptune is a multi-step process:

 1. [**Export CSV from Neo4j**](#export-csv-from-neo4j) – Use the APOC export procedures to export data from Neptune to CSV.
 2. [**Convert CSV**](#convert-csv) – Use the `convert-csv` command-line utility to convert the exported CSV into the Neptune Gremlin bulk load CSV format.
 3. [**Bulk Load into Neptune**](#bulk-load-into-neptune) – Use the Neptune bulk load API to load data into Neptune.
 
### Export CSV from Neo4j

Use the [`apoc.export.csv.all`](https://neo4j.com/docs/labs/apoc/current/export/csv/) procedure from neo4j's [APOC](https://neo4j.com/docs/labs/apoc/current/) library to export data from Neo4j to CSV.

#### 1. Install APOC

Follow the [instructions](https://neo4j.com/docs/labs/apoc/current/introduction/) for installing the APOC library for either Neo4j Desktop or Neo4j Server.

#### 2. Enable exports

Update the _neo4j.conf_ configuration file to enable exports:

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

The path that you specify for the export file will be resolved relative to the Neo4j _import_ directory. `apoc.export.csv.all` creates a single CSV file containing data for all nodes and relationships.
 
### Convert CSV

Use the [`convert-csv`](docs/convert-csv.md) command-line utility to convert the CSV exported from Neo4j into the Neptune Gremlin bulk load CSV format.

The utility has two required parameters: the path to the Neo4j export file and the name of a directory where the converted CSV files will be written. There are also optional parameters that allow you to specify node and relationship multi-valued property policies and turn on data type inferencing.

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

### Bulk Load into Neptune

Use the [Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html) to load data into Neptune from the converted CSV files.
