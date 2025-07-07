/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.neptune.util.CSVUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.Optional;

import static org.junit.Assert.*;

public class VertexMetadataTest {

    private VertexMetadata createVertexMetadata(String columnHeaders, ConversionConfig config) {
        return VertexMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, "", false), config);
    }

    @Test
    public void shouldParseVertexHeadersFromColumnHeaders() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        String expectedHeaders = "~id,~label,address,name,index,txid";

        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders, new ConversionConfig());
        assertEquals(expectedHeaders, String.join(",", vertexMetadata.headers()));
        assertEquals(5, vertexMetadata.lastColumnIndex());
    }

    @Test
    public void shouldIndicateWhetherARecordContainsAVertex() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders,new ConversionConfig());

        CSVRecord vertexRecord = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        CSVRecord edgeRecord = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        assertTrue(vertexMetadata.isVertex(vertexRecord));
        assertFalse(vertexMetadata.isVertex(edgeRecord));
    }

    @Test
    public void shouldWrapRecordWithVertexSpecificIterable() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void shouldRemoveLeadingColonFromLabel() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\":Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void shouldCreateSemicolonDelimitedListOfLabelsForMultipleLabelsInSource(){
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders, new ConversionConfig());

        CSVRecord record = CSVUtils.firstRecord("\"1\",\":Person:Admin\",\"address\",\"name\",\"1\",\"1\"");
        Optional<Iterable<String>> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person;Admin,address,name,1,1";

        assertEquals(expected, String.join(",", vertex.get()));
    }

    @Test
    public void shouldAllowEmptyLabels(){
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders, new ConversionConfig());

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
        VertexMetadata vertexMetadata = createVertexMetadata(headerLine, config);

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
        VertexMetadata vertexMetadata = createVertexMetadata(headerLine, config);

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
        VertexMetadata vertexMetadata = createVertexMetadata(headerLine, config);

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
        VertexMetadata vertexMetadata = createVertexMetadata(headerLine, config);

        // Test that no vertices are skipped when no rules are configured
        String vertexData = "123,Person,John";
        CSVRecord vertexRecord = CSVFormat.DEFAULT.parse(new StringReader(vertexData)).iterator().next();
        assertTrue("Vertex should not be skipped when no rules are configured", vertexMetadata.toIterable(vertexRecord).isPresent());

        assertFalse("Should not have skip rules", config.hasSkipRules());
        assertEquals("Should have 0 skipped vertices", 0, vertexMetadata.getSkippedVertexIds().size());
    }


}