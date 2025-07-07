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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class EdgeMetadataTest {

    private static final Supplier<String> ID_GENERATOR = () -> "edge-id";

    private EdgeMetadata createEdgeMetadata(String columnHeaders) {
        return EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", false), new ConversionConfig(), new HashSet<>());
    }

    private EdgeMetadata createEdgeMetadata(String columnHeaders, ConversionConfig conversionConfig, Set<String> skippedVertexIds) {
        return EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", false), conversionConfig, skippedVertexIds);
    }

    @Test
    public void shouldParseEdgeHeadersFromColumnHeaders() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        String expectedHeaders = "~id,~from,~to,~label,strength,timestamp";

        EdgeMetadata edgeMetadata = createEdgeMetadata(columnHeaders);
        assertEquals(expectedHeaders, String.join(",", edgeMetadata.headers()));
        assertEquals(6, edgeMetadata.firstColumnIndex());
    }

    @Test
    public void shouldIndicateWhetherARecordContainsAnEdge() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        EdgeMetadata edgeMetadata = createEdgeMetadata(columnHeaders);

        CSVRecord vertexRecord = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        CSVRecord edgeRecord = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        assertFalse(edgeMetadata.isEdge(vertexRecord));
        assertTrue(edgeMetadata.isEdge(edgeRecord));
    }

    @Test
    public void shouldWrapRecordWithEdgeSpecificIterable() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        EdgeMetadata edgeMetadata = createEdgeMetadata(columnHeaders);

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
                ID_GENERATOR,
                propertyValueParser, new ConversionConfig(), new HashSet<>());

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
        EdgeMetadata edgeMetadata = createEdgeMetadata(headerLine, config,new HashSet<>());

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
        EdgeMetadata edgeMetadata = createEdgeMetadata(headerLine, config, vertexMetadata.getSkippedVertexIds());

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
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        EdgeMetadata edgeMetadata = createEdgeMetadata(headerLine, config,new HashSet<>());

        // Test that no edges are skipped when no rules are configured
        String edgeData = ",,,1,2,KNOWS";
        CSVRecord edgeRecord = CSVFormat.DEFAULT.parse(new StringReader(edgeData)).iterator().next();
        assertTrue("Edge should not be skipped when no rules are configured", edgeMetadata.toIterable(edgeRecord).isPresent());

        assertTrue("Should not have skip rules", config.getEdgeLabels().isEmpty());
    }

}