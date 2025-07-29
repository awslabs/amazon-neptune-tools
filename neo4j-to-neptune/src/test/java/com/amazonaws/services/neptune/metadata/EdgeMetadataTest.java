/*
Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.TestDataProvider;
import com.amazonaws.services.neptune.util.CSVUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

public class EdgeMetadataTest {
    @Test
    public void shouldParseEdgeHeadersFromColumnHeaders() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        String expectedHeaders = "~id,~from,~to,~label,strength,timestamp";

        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders);
        assertEquals(expectedHeaders, String.join(",", edgeMetadata.headers()));
        assertEquals(6, edgeMetadata.firstColumnIndex());
    }

    @Test
    public void shouldIndicateWhetherARecordContainsAnEdge() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders);

        CSVRecord vertexRecord = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        CSVRecord edgeRecord = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        assertFalse(edgeMetadata.isEdge(vertexRecord));
        assertTrue(edgeMetadata.isEdge(edgeRecord));
    }

    @Test
    public void shouldWrapRecordWithEdgeSpecificIterable() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders);

        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");
        Optional<Iterable<String>> edge = edgeMetadata.toIterable(record);

        String expected = "edge-id,1,2,KNOWS,10,12345";

        assertEquals(expected, String.join(",", edge.get()));
    }

    @Test
    public void shouldUpdateHeadersWithDataTypeInfo() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        PropertyValueParser propertyValueParser = new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", true);

        EdgeMetadata edgeMetadata = EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                propertyValueParser, new ConversionConfig(), new HashSet<>(), new HashMap<String, String>());

        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");
        edgeMetadata.toIterable(record).get().forEach(c -> {});

        String expected = "~id,~from,~to,~label,strength:byte,timestamp:short";

        assertEquals(expected, String.join(",", edgeMetadata.headers()));
    }

    @Test
    public void testEdgeLabelMapping() throws IOException {
        // Create test configuration with label mappings
        File tempFile = File.createTempFile("test-config", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("edgeLabels:\n");
            writer.write("  WORKS_FOR: EMPLOYED_BY\n");
            writer.write("  MANAGES: SUPERVISES\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Create edge metadata
        String headerLine = "_id,_labels,name,_start,_end,_type";
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(headerLine, config,new HashSet<>());

        // Test label mapping
        assertEquals("WORKS_FOR should map to EMPLOYED_BY", "EMPLOYED_BY", edgeMetadata.mapEdgeLabel("WORKS_FOR"));
        assertEquals("MANAGES should map to SUPERVISES", "SUPERVISES", edgeMetadata.mapEdgeLabel("MANAGES"));
        assertEquals("Unmapped labels should remain unchanged", "KNOWS", edgeMetadata.mapEdgeLabel("KNOWS"));

        assertFalse("Should have label mappings", config.getEdgeLabels().isEmpty());
    }

    @Test
    public void testEdgeSkippingByConnectedVertices() throws IOException {
        // Create a temporary YAML file with vertex skip rules
        File tempFile = File.createTempFile("test-edge-skip", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
            writer.write("skipEdges:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TEMP_RELATIONSHIP\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Create mock metadata
        String headerLine = "_id,_labels,name,_start,_end,_type";
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        VertexMetadata vertexMetadata = VertexMetadata.parse(headers,
            new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, " ", false),
                config);
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(headerLine, config, vertexMetadata.getSkippedVertexIds());

        // First, skip vertex 123
        String vertexData = "123,Person,John";
        CSVRecord vertexRecord = CSVFormat.DEFAULT.parse(new StringReader(vertexData)).iterator().next();
        vertexMetadata.shouldSkipVertex(vertexRecord);

        // Test edge records
        String edgeData = ",,,456,789,KNOWS\n,,,123,789,WORKS_FOR\n,,,456,123,MANAGES\n,,,789,456,TEMP_RELATIONSHIP";
        Iterable<CSVRecord> edgeRecords = CSVFormat.DEFAULT.parse(new StringReader(edgeData));

        int recordIndex = 0;
        for (CSVRecord record : edgeRecords) {
            if (recordIndex == 0) {
                assertFalse("Edge between 456-789 should not be skipped", edgeMetadata.shouldSkipEdge(record));
                assertTrue(edgeMetadata.toIterable(record).isPresent());
            } else if (recordIndex == 1) {
                assertTrue("Edge from 123-789 should be skipped (123 is skipped vertex)", edgeMetadata.shouldSkipEdge(record));
                assertFalse(edgeMetadata.toIterable(record).isPresent());
            } else if (recordIndex == 2) {
                assertTrue("Edge from 456-123 should be skipped (123 is skipped vertex)", edgeMetadata.shouldSkipEdge(record));
                assertFalse(edgeMetadata.toIterable(record).isPresent());
            } else if (recordIndex == 3) {
                assertTrue("Edge with TEMP_RELATIONSHIP label should be skipped", edgeMetadata.shouldSkipEdge(record));
                assertFalse(edgeMetadata.toIterable(record).isPresent());
            }
            recordIndex++;
        }
    }

    @Test
    public void testEdgeProcessingWithoutSkipRules() throws IOException {
        // Create empty configuration
        ConversionConfig config = new ConversionConfig();

        // Create edge metadata
        String headerLine = "_id,_labels,name,_start,_end,_type";
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(headerLine, config,new HashSet<>());

        // Test that no edges are skipped when no rules are configured
        String edgeData = ",,,1,2,KNOWS";
        CSVRecord edgeRecord = CSVFormat.DEFAULT.parse(new StringReader(edgeData)).iterator().next();
        assertTrue("Edge should not be skipped when no rules are configured", edgeMetadata.toIterable(edgeRecord).isPresent());

        assertTrue("Should not have skip rules", config.getEdgeLabels().isEmpty());
    }

    @Test
    public void testEdgeIdTransformation() throws IOException {
        // Create a ConversionConfig with edge ID transformation
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "{_type}_{_start}_{_end}");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Create vertex ID mapping
        Map<String, String> vertexIdMap = new HashMap<>();
        vertexIdMap.put("1", "person_1");
        vertexIdMap.put("2", "person_2");

        // Parse the record with the config
        EdgeMetadata edgeMetadata =
            TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>(), vertexIdMap);

        // Process the record
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String edgeData = String.join(",", result.get());

        // Check that the ID was transformed
        assertTrue("ID should be transformed to KNOWS_person_1_person_2",
                edgeData.startsWith("KNOWS_person_1_person_2,"));

        // Check that the from/to fields use the transformed vertex IDs
        assertTrue("From field should be transformed to person_1",
                edgeData.contains(",person_1,"));
        assertTrue("To field should be transformed to person_2",
                edgeData.contains(",person_2,"));
    }

    @Test
    public void testEdgeIdTransformationWithProperties() throws IOException {
        // Create a ConversionConfig with edge ID transformation using properties
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "{_type}_{_start}_{_end}_{strength}");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Create vertex ID mapping
        Map<String, String> vertexIdMap = new HashMap<>();
        vertexIdMap.put("1", "person_1");
        vertexIdMap.put("2", "person_2");

        // Parse the record with the config
        EdgeMetadata edgeMetadata =
            TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>(), vertexIdMap);

        // Process the record
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String edgeData = String.join(",", result.get());

        // Check that the ID was transformed with property
        assertTrue("ID should be transformed to KNOWS_person_1_person_2_10",
                edgeData.startsWith("KNOWS_person_1_person_2_10,"));
    }

    @Test
    public void testEdgeIdTransformationWithLabelMapping() throws IOException {
        // Create a ConversionConfig with edge ID transformation and label mapping
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "{_type}_{_start}_{_end}");
        config.getEdgeLabels().put("KNOWS", "FRIEND_OF");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Create vertex ID mapping
        Map<String, String> vertexIdMap = new HashMap<>();
        vertexIdMap.put("1", "person_1");
        vertexIdMap.put("2", "person_2");

        // Parse the record with the config
        EdgeMetadata edgeMetadata =
            TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>(), vertexIdMap);

        // Process the record
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        List<String> values = new ArrayList<>();
        result.get().forEach(values::add);

        // Check that the ID was transformed with mapped label
        assertEquals("FRIEND_OF_person_1_person_2", values.get(0));

        // Check that the label was mapped (at index 3)
        assertEquals("FRIEND_OF", values.get(3));
    }

    @Test
    public void testEdgeIdTransformationWithGremlinFormat() throws IOException {
        // Create a ConversionConfig with edge ID transformation using Gremlin format
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "e_{~label}_{~from}_{~to}");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Create vertex ID mapping
        Map<String, String> vertexIdMap = new HashMap<>();
        vertexIdMap.put("1", "person_1");
        vertexIdMap.put("2", "person_2");

        // Parse the record with the config
        EdgeMetadata edgeMetadata =
            TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>(), vertexIdMap);

        // Process the record
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String edgeData = String.join(",", result.get());

        // Check that the ID was transformed
        assertTrue("ID should be transformed to e_KNOWS_person_1_person_2",
                edgeData.startsWith("e_KNOWS_person_1_person_2,"));
    }

    @Test
    public void testEdgeIdTransformationWithSkippedVertices() throws IOException {
        // Create a ConversionConfig with edge ID transformation
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "{_type}_{_start}_{_end}");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Create vertex ID mapping and skipped vertices
        Map<String, String> vertexIdMap = new HashMap<>();
        vertexIdMap.put("1", "person_1");
        Set<String> skippedVertexIds = new HashSet<>();
        skippedVertexIds.add("2");

        // Parse the record with the config
        EdgeMetadata edgeMetadata =
            TestDataProvider.createEdgeMetadata(columnHeaders, config, skippedVertexIds, vertexIdMap);

        // Process the record - this should be skipped
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result is empty (edge is skipped)
        assertFalse("Edge should be skipped because vertex 2 is skipped", result.isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEdgeIdTransformationWithInvalidProperty() throws IOException {
        // Create a ConversionConfig with edge ID transformation using non-existent property
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "{_type}_{invalid_property}_{_start}");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Parse the record with the config
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>());

        // This should throw IllegalArgumentException when iterator is consumed
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);
        if (result.isPresent()) {
            String.join(",", result.get()); // Force consumption to trigger exception
        }
    }

    @Test
    public void testEdgeIdTransformationWithNoTemplate() throws IOException {
        // Create a ConversionConfig without edge ID transformation
        ConversionConfig config = new ConversionConfig();

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Parse the record with the config
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>());

        // Process the record
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result uses default ID generator
        assertTrue(result.isPresent());
        String edgeData = String.join(",", result.get());
        assertTrue("Should use default edge-id", edgeData.startsWith("edge-id,"));
    }

    @Test
    public void testEdgeIdTransformationWithStaticTemplate() throws IOException {
        // Create a ConversionConfig with static template (no placeholders)
        ConversionConfig config = new ConversionConfig();
        config.getEdgeIdTransformation().put("~id", "static_edge_id");

        // Create a CSV record with edge data
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        // Parse the record with the config
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders, config, new HashSet<>());

        // Process the record
        Optional<Iterable<String>> result = edgeMetadata.toIterable(record);

        // Verify the result uses static template
        assertTrue(result.isPresent());
        String edgeData = String.join(",", result.get());
        assertTrue("Should use static template", edgeData.startsWith("static_edge_id,"));
    }

    @Test
    public void testMapEdgeLabelWithNullAndEmpty() throws IOException {
        ConversionConfig config = new ConversionConfig();
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata("_id,_labels,_start,_end,_type", config, new HashSet<>());

        // Test null label
        assertEquals(null, edgeMetadata.mapEdgeLabel(null));

        // Test empty label
        assertEquals("", edgeMetadata.mapEdgeLabel(""));

        // Test whitespace-only label
        assertEquals("   ", edgeMetadata.mapEdgeLabel("   "));
    }

    @Test
    public void testShouldSkipEdgeWithInsufficientData() throws IOException {
        ConversionConfig config = new ConversionConfig();
        String headerLine = "_id,_labels,_start,_end,_type";
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(headerLine, config, new HashSet<>());

        // Create a record with insufficient columns
        CSVRecord shortRecord = CSVUtils.firstRecord("1,2");

        // Should not skip due to insufficient data (returns false for safety)
        assertFalse("Should not skip edge with insufficient data", edgeMetadata.shouldSkipEdge(shortRecord));
    }

    @Test
    public void testIsEdgeWithInsufficientColumns() throws IOException {
        String columnHeaders = "\"_id\",\"_labels\",\"_start\",\"_end\",\"_type\"";
        EdgeMetadata edgeMetadata = TestDataProvider.createEdgeMetadata(columnHeaders);

        // Test with record that has fewer columns than expected
        CSVRecord shortRecord = CSVUtils.firstRecord("1,2");
        assertFalse("Should return false for records with insufficient columns", edgeMetadata.isEdge(shortRecord));
    }
}
