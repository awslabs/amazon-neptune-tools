# Conversion Configuration

The `convert-csv` utility supports advanced configuration through YAML files that enable label mapping, ID transformation and data filtering during the Neo4j to Neptune conversion process.

## Usage

Use the `--conversion-config` parameter to specify a YAML file containing the label mapping, ID transformation and filtering rules:

```bash
java -jar neo4j-to-neptune.jar convert-csv \
  -i /path/to/neo4j-export.csv \
  -d /path/to/output \
  --conversion-config conversion-config.yaml \
  [other options]
```

## Configuration File Structure

The configuration file should be in YAML format, using camelCase, with sections for label mapping and record filtering:

```yaml
# Label Mappings
vertexLabels:
  OldVertexLabel: NewVertexLabel
  Person: Individual
  Company: Organization
  Product: Item

edgeLabels:
  OLD_RELATIONSHIP: NEW_RELATIONSHIP
  WORKS_FOR: EMPLOYED_BY
  LIVES_IN: RESIDES_IN
  KNOWS: CONNECTED_TO

# ID Transformations
vertexIdTransformation:
  ~id: "{template}"

edgeIdTransformation:
  ~id: "{template}"

# Filtering Rules
skipVertices:
  byId:
    - "vertex_id"
  byLabel:
    - "LabelName"

skipEdges:
  byLabel:
    - "RELATIONSHIP_TYPE"
```

**Note:** label only for edge as Neo4j default csv export doesn't include original edge IDs.

**Tip:** To avoid long bullet lists, one could use the alternative YAML format for list input:_
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

Map Neo4j node labels to Neptune-compatible vertex labels:

```yaml
vertexLabels:
  Person: Individual
  Company: Organization
  User: Customer
  Account: Profile
```

### Edge Labels
- Neo4j relationships have a single type/label
- The entire label is mapped if a mapping exists
- Unmapped labels are kept as-is

Map Neo4j relationship types to Neptune-compatible edge labels:

```yaml
edgeLabels:
  WORKS_FOR: EMPLOYED_BY
  KNOWS: CONNECTED_TO
  MANAGES: SUPERVISES
  OWNS: POSSESSES
```
**Example:**
- Input: `WORKS_FOR` with mapping `WORKS_FOR: EMPLOYED_BY`
- Output: `EMPLOYED_BY`

**Notes:**
- Label mappings are applied before ID transformations
- Unmapped labels remain unchanged
- Case-sensitive matching

## ID Transformation

Transform vertex and edge IDs using template patterns that reference original data fields.

### Vertex ID Transformation

Transform vertex IDs using templates that can reference:

| Placeholder | Description | Example Value |
|-------------|-------------|---------------|
| `{_id}` | Original Neo4j vertex ID | `"123"` |
| `{_labels}` | Original Neo4j Vertex labels | `"Person_Employee"` |
| `{~id}` | Gremlin alias vertex ID | `"123"` |
| `{~label}` | Gremlin alias vertex labels | `"Person_Employee"` |
| `{property_name}` | Any vertex property from CSV | `"John"` |

**Examples:**

```yaml
vertexIdTransformation:
  # Label-based ID
  ~id: "{_labels}_{_id}!{~labels}_{~id}"
  # Input: ID=123, Labels=":Person:Employee"
  # Output: "Person_Employee_123!Person_Employee_123"

  # Property-based ID
  ~id: "{_labels}_{name}_{_id}"
  # Input: ID=123, Labels=":Person", name="John"
  # Output: "Person_John_123"

  # Static prefix
  ~id: "vertex_{_id}"
  # Input: ID=123
  # Output: "vertex_123"
```

### Edge ID Transformation

Transform edge IDs using templates that can reference:

| Placeholder | Description | Example Value |
|-------------|-------------|---------------|
| `{_id}` | Original Neo4j edge ID | `"456"` |
| `{_type}` | Edge type/label (Neo4j format) | `"KNOWS"` |
| `{_start}` | Source vertex ID (original) | `"123"` |
| `{_end}` | Target vertex ID (original) | `"789"` |
| `{~id}` | Edge id (after mapping) | `"456"` |
| `{~label}` | Edge label (after mapping) | `"CONNECTED_TO"` |
| `{~from}` | Source vertex ID (after transformation) | `"Person_123"` |
| `{~to}` | Target vertex ID (after transformation) | `"Person_789"` |
| `{property_name}` | Any edge property from CSV | `"0.8"` (for weight property) |

**Examples:**

```yaml
edgeIdTransformation:
  # Neo4j format references
  ~id: "{_type}_{_start}_{_end}"
  # Input: type="KNOWS", start=123, end=789
  # Output: "KNOWS_123_789"

  # Gremlin format references (uses transformed vertex IDs)
  ~id: "e_{~label}_{~from}_{~to}"
  # Input: label="CONNECTED_TO", from="Person_123", to="Person_789"
  # Output: "e_CONNECTED_TO_Person_123_Person_789"

  # Property-based ID
  ~id: "{_type}_{_start}_{_end}_{weight}"
  # Input: type="KNOWS", start=123, end=789, weight=0.8
  # Output: "KNOWS_123_789_0.8"
```

## Data Filtering

### Skip Vertices

You can skip vertices during conversion in two ways:
1. **By ID**: Skip specific vertices by their unique identifiers (note that IDs are treated as strings)
2. **By Label**: Skip all vertices that have any of the specified labels

```yaml
skipVertices:
  byId:
    - "test_vertex_1"
    - "temp_123"
    - "debug_node_456"
  byLabel:
    - "TestData"
    - "Deprecated"
    - "SystemInternal"
```
**Note:**  When a vertex is skipped:
- The vertex is not written to the output vertices CSV file
- The skipped vertex ID is tracked for edge filtering

#### Automatic Edge Filtering
When vertices are skipped, all edges connected to that vertex (incoming and outgoing) are automatically skipped to maintains graph consistency and prevents dangling edge references

**Example:**
- If vertex `123` is skipped
- Edge `456 -> 123` is automatically skipped
- Edge `123 -> 789` is automatically skipped
- Edge `456 -> 789` is preserved (if neither 456 nor 789 are skipped)

### Skip Edges

Skip specific edges during conversion:

```yaml
skipEdges:
  byLabel:
    - "TEMP_RELATIONSHIP"
    - "DEBUG_LINK"
    - "TEST_CONNECTION"
```

**Note:** label only for edge as Neo4j default csv export doesn't include original edge IDs.

## Processing Order

The conversion utility processes configuration in this order:

1. **Label Mapping**: Apply vertex and edge label mappings
2. **Filtering**: Skip vertices and edges based on rules
3. **ID Transformation**: Transform remaining vertex and edge IDs
3a. **Two-Pass Processing**:
    - Pass 1: Process vertices, build ID mapping
    - Pass 2: Process edges using vertex ID mapping

**Complete Example**: `/docs/example-conversion-config.yaml`

## Error Handling

The utility provides clear error messages for common issues:

- **Invalid property references**: `Property {invalid_property} not found in CSV headers`
- **Missing required fields**: `Template references {_labels} but no label column exists`
- **Malformed YAML**: Clear syntax error messages with line numbers

## Best Practices

1. **Test with small datasets first**: Validate configuration with subset of data
2. **Use meaningful ID patterns**: Choose patterns that make sense for your domain
3. **Consider downstream systems**: Ensure transformed IDs are compatible
4. **Document your mappings**: Keep track of transformations for future reference
5. **Validate results**: Check converted CSV files before bulk loading
