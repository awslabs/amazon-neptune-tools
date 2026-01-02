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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

public class VertexMetadataTest {

    @Test
    public void shouldParseVertexHeadersFromColumnHeaders() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        String expectedHeaders = "~id,~label,address,name,index,txid";

        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, new ConversionConfig());
        assertEquals(expectedHeaders, String.join(",", vertexMetadata.headers()));
        assertEquals(5, vertexMetadata.lastColumnIndex());
    }

    @Test
    public void shouldIndicateWhetherARecordContainsAVertex() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders,new ConversionConfig());

        CSVRecord vertexRecord = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        CSVRecord edgeRecord = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        assertTrue(vertexMetadata.isVertex(vertexRecord));
        assertFalse(vertexMetadata.isVertex(edgeRecord));
    }

    @Test
    public void shouldWrapRecordWithVertexSpecificIterable() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void shouldRemoveLeadingColonFromLabel() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\":Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void shouldCreateSemicolonDelimitedListOfLabelsForMultipleLabelsInSource(){
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\":Person:Admin\",\"address\",\"name\",\"1\",\"1\"");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person;Admin,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void shouldAllowEmptyLabels(){
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\"\",\"address\",\"name\",\"1\",\"1\"");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void testVertexLabelMapping() throws IOException {
        // Create test configuration with label mappings
        File tempFile = File.createTempFile("test-config", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Company: Organization\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Create vertex metadata
        String headerLine = "_id,_labels,name";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(headerLine, config);

        // Test label mapping
        assertEquals("Person should map to Individual", "Individual", vertexMetadata.mapVertexLabels("Person"));
        assertEquals("Company should map to Organization", "Organization", vertexMetadata.mapVertexLabels("Company"));
        assertEquals("Multiple labels should be mapped", "Individual;Organization",
                vertexMetadata.mapVertexLabels("Person:Company"));
        assertEquals("Unmapped labels should remain unchanged", "Employee", vertexMetadata.mapVertexLabels("Employee"));

    }

    @Test
    public void testVertexSkippingById() throws IOException {
        // Create a temporary YAML file with vertex ID skip rules
        File tempFile = File.createTempFile("test-skip", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
            writer.write("    - \"456\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Create mock vertex metadata
        String headerLine = "_id,_labels,name";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(headerLine, config);

        // Test vertex records
        String vertexData = "123,Person,John\n456,Company,Acme\n789,Person,Jane";
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(vertexData));

        int recordIndex = 0;
        for (CSVRecord record : records) {
            if (recordIndex == 0) {
                assertTrue("Vertex 123 should be skipped", vertexMetadata.shouldSkipVertex(record));
            } else if (recordIndex == 1) {
                assertTrue("Vertex 456 should be skipped", vertexMetadata.shouldSkipVertex(record));
            } else if (recordIndex == 2) {
                assertFalse("Vertex 789 should not be skipped", vertexMetadata.shouldSkipVertex(record));
            }
            recordIndex++;
        }

        assertEquals(2, vertexMetadata.getSkippedVertexIds().size());
        assertTrue(vertexMetadata.getSkippedVertexIds().contains("123"));
        assertTrue(vertexMetadata.getSkippedVertexIds().contains("456"));
    }

    @Test
    public void testVertexSkippingByLabel() throws IOException {
        // Create a temporary YAML file with vertex label skip rules
        File tempFile = File.createTempFile("test-skip-label", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
            writer.write("    - \"Deprecated\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Create mock vertex metadata
        String headerLine = "_id,_labels,name";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(headerLine, config);

        // Test vertex records with different labels
        String vertexData = "1,Person,John\n2,TestData,Test\n3,Person:Deprecated,Old\n4,Company,Acme";
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(vertexData));

        int recordIndex = 0;
        for (CSVRecord record : records) {
            if (recordIndex == 0) {
                assertFalse("Person vertex should not be skipped", vertexMetadata.shouldSkipVertex(record));
            } else if (recordIndex == 1) {
                assertTrue("TestData vertex should be skipped", vertexMetadata.shouldSkipVertex(record));
            } else if (recordIndex == 2) {
                assertTrue("Deprecated vertex should be skipped", vertexMetadata.shouldSkipVertex(record));
            } else if (recordIndex == 3) {
                assertFalse("Company vertex should not be skipped", vertexMetadata.shouldSkipVertex(record));
            }
            recordIndex++;
        }

        assertEquals(2, vertexMetadata.getSkippedVertexIds().size());
    }

    @Test
    public void testVertexProcessingWithoutSkipRules() throws IOException {
        // Create empty configuration
        ConversionConfig config = new ConversionConfig();

        // Create vertex metadata
        String headerLine = "_id,_labels,name";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(headerLine, config);

        // Test that no vertices are skipped when no rules are configured
        String vertexData = "123,Person,John";
        CSVRecord vertexRecord = CSVFormat.DEFAULT.parse(new StringReader(vertexData)).iterator().next();
        assertTrue("Vertex should not be skipped when no rules are configured", vertexMetadata.toIterable(vertexRecord).isPresent());

        assertFalse("Should not have skip rules", config.hasSkipRules());
        assertEquals("Should have 0 skipped vertices", 0, vertexMetadata.getSkippedVertexIds().size());
    }

    @Test
    public void testVertexIdTransformation() throws IOException {
        // Create a ConversionConfig with vertex ID transformation
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{_id}");

        String columnHeaders = "\"_id\",\"_labels\",\"name\",\"age\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        // Create a CSV record with vertex data
        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"John\",\"30\"");

        // Process the record
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String vertexData = String.join(",", result.get());

        // Check that the ID was transformed
        assertTrue("ID should be transformed to Person_1", vertexData.startsWith("Person_1,"));

        // Check that the ID mapping was stored
        assertTrue("ID mapping should be stored", vertexMetadata.getVertexIdMap().containsKey("1"));
        assertEquals("Person_1", vertexMetadata.getVertexIdMap().get("1"));
    }

    @Test
    public void testVertexIdTransformationWithProperties() throws IOException {
        // Create a ConversionConfig with vertex ID transformation using properties
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{name}_{_id}");

        String columnHeaders = "\"_id\",\"_labels\",\"name\",\"age\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        // Create a CSV record with vertex data
        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"John\",\"30\"");

        // Process the record
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String vertexData = String.join(",", result.get());

        // Check that the ID was transformed
        assertTrue("ID should be transformed to Person_John_1", vertexData.startsWith("Person_John_1,"));

        // Check that the ID mapping was stored
        assertTrue("ID mapping should be stored", vertexMetadata.getVertexIdMap().containsKey("1"));
        assertEquals("Person_John_1", vertexMetadata.getVertexIdMap().get("1"));
    }

    @Test
    public void testVertexIdTransformationWithMultipleLabels() throws IOException {
        // Create a ConversionConfig with vertex ID transformation
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{_id}");

        String columnHeaders = "\"_id\",\"_labels\",\"name\",\"age\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        // Create a CSV record with vertex data and multiple labels
        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person:Employee\",\"John\",\"30\"");

        // Process the record
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String vertexData = String.join(",", result.get());

        // Check that the ID was transformed with labels joined by underscore
        assertTrue("ID should be transformed to Person_Employee_1", vertexData.startsWith("Person_Employee_1,"));
    }

    @Test
    public void testVertexIdTransformationWithLabelMapping() throws IOException {
        // Create a ConversionConfig with vertex ID transformation and label mapping
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{_id}");
        config.getVertexLabels().put("Person", "Individual");

        String columnHeaders = "\"_id\",\"_labels\",\"name\",\"age\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        // Create a CSV record with vertex data
        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"John\",\"30\"");

        // Process the record
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);

        // Verify the result
        assertTrue(result.isPresent());
        String vertexData = String.join(",", result.get());

        // Check that the ID was transformed with mapped label
        assertTrue("ID should be transformed to Individual_1", vertexData.startsWith("Individual_1,"));

        // Check that the label was mapped
        assertTrue("Label should be mapped to Individual", vertexData.contains(",Individual,"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testVertexIdTransformationWithInvalidProperty() throws IOException {
        // Create a ConversionConfig with vertex ID transformation using non-existent property
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{non_existent_property}_{_id}");

        String columnHeaders = "\"_id\",\"_labels\",\"name\",\"age\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        // Create a CSV record with vertex data
        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"John\",\"30\"");

        // This should throw IllegalArgumentException when the iterator is consumed
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);
        if (result.isPresent()) {
            // Force consumption of the iterator to trigger the exception
            String.join(",", result.get());
        }
    }

    @Test
    public void testVertexIdTransformationWithNullAndEmptyId() throws IOException {
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{_id}");

        String columnHeaders = "\"_id\",\"_labels\",\"name\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        // Test null ID
        CSVRecord nullRecord = CSVUtils.firstRecord(",\"Person\",\"John\"");
        Optional<Iterable<String>> result1 = vertexMetadata.toIterable(nullRecord);
        assertTrue(result1.isPresent());
        String data1 = String.join(",", result1.get());
        assertTrue("Should handle empty ID", data1.startsWith(","));

        // Test whitespace-only ID
        CSVRecord whitespaceRecord = CSVUtils.firstRecord("\"   \",\"Person\",\"John\"");
        Optional<Iterable<String>> result2 = vertexMetadata.toIterable(whitespaceRecord);
        assertTrue(result2.isPresent());
        String data2 = String.join(",", result2.get());
        assertTrue("Should handle whitespace ID", data2.startsWith("   ,"));
    }

    @Test
    public void testVertexIdTransformationWithStaticTemplate() throws IOException {
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "static_vertex_id");

        String columnHeaders = "\"_id\",\"_labels\",\"name\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"John\"");
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);

        assertTrue(result.isPresent());
        String vertexData = String.join(",", result.get());
        assertTrue("Should use static template", vertexData.startsWith("static_vertex_id,"));
    }

    @Test
    public void testVertexIdTransformationWithNoTemplate() throws IOException {
        ConversionConfig config = new ConversionConfig();
        // No ID transformation configured

        String columnHeaders = "\"_id\",\"_labels\",\"name\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        CSVRecord record = CSVUtils.firstRecord("\"original_id\",\"Person\",\"John\"");
        Optional<Iterable<String>> result = vertexMetadata.toIterable(record);

        assertTrue(result.isPresent());
        String vertexData = String.join(",", result.get());
        assertTrue("Should use original ID when no template", vertexData.startsWith("original_id,"));
    }

    @Test
    public void testMapVertexLabelsWithNullAndEmpty() throws IOException {
        ConversionConfig config = new ConversionConfig();
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata("_id,_labels,name", config);

        // Test null labels
        assertEquals(null, vertexMetadata.mapVertexLabels(null));

        // Test empty labels
        assertEquals("", vertexMetadata.mapVertexLabels(""));

        // Test whitespace-only labels
        assertEquals("   ", vertexMetadata.mapVertexLabels("   "));
    }

    @Test
    public void testShouldSkipVertexWithEmptyLabels() throws IOException {
        File tempFile = File.createTempFile("test-skip-empty-labels", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata("_id,_labels,name", config);

        // Test vertex with empty labels
        CSVRecord emptyLabelsRecord = CSVUtils.firstRecord("1,,John");
        assertFalse("Should not skip vertex with empty labels", vertexMetadata.shouldSkipVertex(emptyLabelsRecord));

        // Test vertex with null labels (single column record)
        CSVRecord singleColumnRecord = CSVUtils.firstRecord("1");
        assertFalse("Should not skip vertex with insufficient columns", vertexMetadata.shouldSkipVertex(singleColumnRecord));
    }

    @Test
    public void testVertexIdMappingStorage() throws IOException {
        ConversionConfig config = new ConversionConfig();
        config.getVertexIdTransformation().put("~id", "{_labels}_{_id}");

        String columnHeaders = "\"_id\",\"_labels\",\"name\"";
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata(columnHeaders, config);

        CSVRecord record1 = CSVUtils.firstRecord("\"1\",\"Person\",\"John\"");
        CSVRecord record2 = CSVUtils.firstRecord("\"2\",\"Company\",\"ACME\"");

        // Process records and consume the iterators to trigger ID mapping storage
        Optional<Iterable<String>> result1 = vertexMetadata.toIterable(record1);
        if (result1.isPresent()) {
            String.join(",", result1.get()); // Consume iterator
        }
        Optional<Iterable<String>> result2 = vertexMetadata.toIterable(record2);
        if (result2.isPresent()) {
            String.join(",", result2.get()); // Consume iterator
        }

        // Verify ID mappings are stored
        Map<String, String> idMap = vertexMetadata.getVertexIdMap();
        assertEquals("Person_1", idMap.get("1"));
        assertEquals("Company_2", idMap.get("2"));
        assertEquals(2, idMap.size());
    }

    @Test
    public void testSkippedVertexIdsTracking() throws IOException {
        File tempFile = File.createTempFile("test-skip-tracking", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        VertexMetadata vertexMetadata = TestDataProvider.createVertexMetadata("_id,_labels,name", config);

        // Process vertices that should be skipped
        CSVRecord skipById = CSVUtils.firstRecord("123,Person,John");
        CSVRecord skipByLabel = CSVUtils.firstRecord("456,TestData,Test");
        CSVRecord normalVertex = CSVUtils.firstRecord("789,Person,Jane");

        vertexMetadata.shouldSkipVertex(skipById);
        vertexMetadata.shouldSkipVertex(skipByLabel);
        vertexMetadata.shouldSkipVertex(normalVertex);

        // Verify skipped vertex IDs are tracked
        Set<String> skippedIds = vertexMetadata.getSkippedVertexIds();
        assertTrue("Should track vertex skipped by ID", skippedIds.contains("123"));
        assertTrue("Should track vertex skipped by label", skippedIds.contains("456"));
        assertFalse("Should not track normal vertex", skippedIds.contains("789"));
        assertEquals(2, skippedIds.size());
    }
}
