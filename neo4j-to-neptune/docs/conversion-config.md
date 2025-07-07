# Label Mapping and Record Filtering

The `convert-csv` utility supports mapping vertex and edge labels and filtering records during the conversion process from Neo4j to Neptune format. This feature allows you to rename labels to follow different naming conventions, resolve naming conflicts, and exclude specific vertices and edges from the conversion.

## Usage

Use the `--conversion-config` parameter to specify a YAML file containing the label mappings and filtering rules:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /tmp/neo4j-export.csv \
  -d output \
  --conversion-config  config.yaml \
  --infer-types
```

## YAML Configuration Format

The configuration file should be in YAML format, using camelCase, with sections for label mapping and record filtering:

```yaml
# Vertex label mappings
vertexLabels:
  OldVertexLabel: NewVertexLabel
  Person: Individual
  Company: Organization
  Product: Item

# Edge label mappings  
edgeLabels:
  OLD_RELATIONSHIP_TYPE: NEW_RELATIONSHIP_TYPE
  WORKS_FOR: EMPLOYED_BY
  LIVES_IN: RESIDES_IN
  KNOWS: CONNECTED_TO

# Skip vertices configuration
skipVertices:
  # Skip vertices by their specific IDs
  byId:
    - "vertex_123"
    - "vertex_456"
    - "user_999"
  
  # Skip vertices by their labels
  byLabel:
    - "TestData"
    - "Deprecated"
    - "TempNode"

# Skip edges configuration
skipEdges:
  # Skip edges by their relationship types
  byLabel:
    - "TEMP_RELATIONSHIP"
    - "DEBUG_LINK"
    - "OLD_CONNECTION"
```
_**Note:** label only for edge as Neo4j default csv export doesn't include original edge IDs._

_**Tip:** To avoid long bullet lists, one could use the alternative YAML format for list input:_
```yaml
skipVertices:
  byId: ["vertex_123","vertex_456","user_999"]
```

## Label Mapping

### Vertex Labels

- Neo4j vertices can have multiple labels separated by colons (e.g., `Person:Employee`)
- Each individual label is mapped independently if a mapping exists
- Unmapped labels are kept as-is
- The output uses semicolons as separators (Neptune format)

**Example:**
- Input: `Person:Employee` with mapping `Person: Individual`
- Output: `Individual;Employee`

### Edge Labels

- Neo4j relationships have a single type/label
- The entire label is mapped if a mapping exists
- Unmapped labels are kept as-is

**Example:**
- Input: `WORKS_FOR` with mapping `WORKS_FOR: EMPLOYED_BY`
- Output: `EMPLOYED_BY`

## Record Filtering

### Skip Vertices

You can skip vertices in two ways:

1. **By ID**: Skip specific vertices by their unique identifiers (note that IDs are treated as strings)
2. **By Label**: Skip all vertices that have any of the specified labels

When a vertex is skipped:
- The vertex is not written to the output vertices CSV file
- All edges connected to that vertex (incoming and outgoing) are automatically skipped
- The skipped vertex ID is tracked for edge filtering

### Skip Edges

You can skip edges in following way:

1. **By Label**: Skip all edges of the specified relationship types

_**Note:** label only for edge as Neo4j default csv export doesn't include original edge IDs._

### Automatic Edge Filtering

When vertices are skipped, the system automatically skips any edges that connect to or from those vertices. This ensures data consistency in the output.

**Example:**
- If vertex `123` is skipped
- Edge `456 -> 123` is automatically skipped
- Edge `123 -> 789` is automatically skipped
- Edge `456 -> 789` is preserved (if neither 456 nor 789 are skipped)

## Behavior

- **Optional**: All configuration sections are optional
- **Partial configurations**: You can use only label mapping, only filtering, or both
- **Case-sensitive**: All mappings and filters are case-sensitive
- **Whitespace handling**: Leading and trailing whitespace is trimmed
- **Empty sections**: Any section can be omitted if not needed

## Output Statistics

When filtering is enabled, the conversion process reports:
- Number of vertices processed and skipped
- Number of edges processed and skipped
- Summary of skip rules applied

**Example output:**
```
Vertices: 1500
Edges   : 2300
Skipped vertices: 25
Skipped edges  : 78
Skip rules: 3 vertex IDs, 2 vertex labels, 1 edge labels
Output  : output/1234567890
```

## Complete Example

Given a Neo4j export with:
- Vertices: `Person:Manager`, `TestData:Person`, `Company`
- Edges: `REPORTS_TO`, `TEMP_RELATIONSHIP`

And the following configuration:

```yaml
vertexLabels:
  Person: Individual
  Manager: Supervisor
  Company: Organization

edgeLabels:
  REPORTS_TO: MANAGED_BY

skipVertices:
  byLabel:
    - "TestData"

skipEdges:
  byLabel:
    - "TEMP_RELATIONSHIP"
```

The conversion will produce:
- Vertex `Person:Manager` → `Individual;Supervisor`
- Vertex `TestData:Person` → **skipped**
- Vertex `Company` → `Organization`
- Edge `REPORTS_TO` → `MANAGED_BY`
- Edge `TEMP_RELATIONSHIP` → **skipped**
- Any edges connected to the skipped `TestData:Person` vertex → **skipped**

## Error Handling

- If the mapping file doesn't exist, the conversion will fail with an error
- If the YAML file is malformed, the conversion will fail with a parsing error
- If the YAML file exists but is empty or has no configurations, the conversion proceeds without modifications
- Invalid skip rules (e.g. non-existent IDs, mismatched labels) are silently ignored
