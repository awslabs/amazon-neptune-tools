# openCypher Compatibility Checker

A migration helper tool that validates openCypher queries for compatibility with Amazon Neptune, identifying unsupported functions and clauses to help assess migration effort from Neo4j to Neptune.

## Overview

This tool analyzes openCypher queries and reports:
- Compatibility status for each query
- Specific unsupported functions/clauses with their positions
- Suggested replacements where available
- Detailed error descriptions

Supports validation for both **Neptune Analytics (NA)** and **Neptune Database (NDB)**.

To select the appropriate version go to the [Releases](https://github.com/awslabs/amazon-neptune-tools/releases) page.

For Neptune Analytics, find the release [opencypher-compatability-checker-analytics](https://github.com/awslabs/amazon-neptune-tools/releases/tag/opencypher-compatability-checker-analytics)

For Neptune Database, find the [release](https://github.com/awslabs/amazon-neptune-tools/releases) tagged with the version of your Neptune Database cluster.  Support for openCypher clauses and functions are version dependant so please ensure you select the correct version. 

## Prerequisites

- **Java 17** or higher
- Pre-built JAR file: `NeptuneNeo4jMigrationHelper-<VERSION>.jar` located on the [Releases](https://github.com/awslabs/amazon-neptune-tools/releases) page.

## Installation & Build

The tool is distributed as a fat JAR created during the build process. No additional dependencies are required at runtime other than Java 17.

## Usage

### Basic Command

```bash
java -jar NeptuneNeo4jMigrationHelper-<VERSION>.jar --input <input-file> [--output <output-file>]
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--input` | Yes | Path to JSON file containing queries to validate.  A sample is provided in [input.json](./input.json)|
| `--output` | No | Path for JSON output file. If omitted, prints to stdout.  A sample of the output is provided in [output.json](./output.json) |

### Examples

```bash
# Validate queries and save results to file
java -jar NeptuneNeo4jMigrationHelper-1.0.jar --input queries.json --output results.json

# Validate queries and print to console
java -jar NeptuneNeo4jMigrationHelper-1.0.jar --input queries.json
```

## Configuration

### Logging

Control log verbosity with the `LOG_LEVEL` environment variable:

```bash
export LOG_LEVEL=DEBUG    # Detailed debugging information
export LOG_LEVEL=INFO     # Default level
export LOG_LEVEL=WARN     # Warnings only  
export LOG_LEVEL=ERROR    # Errors only
```

A log file `migration-helper-<current-date>.log` is created in the execution directory.

## Input Format

Create a JSON file with the following structure:

```json
{
  "targetSystem": "NA",
  "queries": [
    {
      "id": 1,
      "query": "MATCH (n:Person) RETURN n LIMIT 10"
    },
    {
      "id": 2, 
      "query": "RETURN apoc.coll.intersection([1,2,3], [2,3,4])"
    }
  ]
}
```

### Fields

- **targetSystem**: Target Neptune system
  - `"NA"` - Neptune Analytics
  - `"NDB"` - Neptune Database
- **queries**: Array of query objects
  - **id**: Unique identifier for the query
  - **query**: openCypher query string to validate

## Output Format

The tool generates a JSON report with validation results:

```json
{
  "results": [
    {
      "id": 1,
      "supported": true,
      "errorDefinitions": []
    },
    {
      "id": 2,
      "supported": false,
      "errorDefinitions": [
        {
          "position": "line 1, column 8 (offset: 7)",
          "name": "apoc.coll.intersection",
          "replacement": "collintersection",
          "description": "apoc.coll.intersection is not supported in this release but try replacing with collintersection"
        }
      ]
    }
  ]
}
```

### Result Fields

- **id**: Matches the input query ID
- **supported**: Boolean indicating Neptune compatibility
- **errorDefinitions**: Array of compatibility issues
  - **position**: Location of the issue in the query
  - **name**: Unsupported function/clause name
  - **replacement**: Suggested Neptune-compatible alternative (if available)
  - **description**: Detailed explanation of the compatibility issue

## Troubleshooting

### Common Errors

1. **Invalid input file**: Ensure JSON is properly formatted
2. **Java version**: Requires Java 17+
3. **File permissions**: Ensure read access to input file and write access to output directory

### Getting Help

- Check log files for detailed error information
- Verify input JSON format matches the specification
- Ensure target system value is either "NA" or "NDB"

For all issues with the tool please file an issue on this GitHub repository.

## License

This tool is part of the AWS Samples repository and follows the same licensing terms.