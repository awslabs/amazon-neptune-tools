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

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class ConversionConfigTest {
    @Test
    public void testDefaultConstructor() {
        // Test default constructor creates properly initialized object
        ConversionConfig config = new ConversionConfig();

        assertNotNull("Vertex labels should be initialized", config.getVertexLabels());
        assertNotNull("Edge labels should be initialized", config.getEdgeLabels());
        assertNotNull("Vertex ID transformation should be initialized", config.getVertexIdTransformation());
        assertNotNull("Edge ID transformation should be initialized", config.getEdgeIdTransformation());
        assertNotNull("Skip vertices should be initialized", config.getSkipVertices());
        assertNotNull("Skip edges should be initialized", config.getSkipEdges());

        assertTrue("All collections should be empty by default", config.getVertexLabels().isEmpty());
        assertTrue("All collections should be empty by default", config.getEdgeLabels().isEmpty());
        assertTrue("All collections should be empty by default", config.getVertexIdTransformation().isEmpty());
        assertTrue("All collections should be empty by default", config.getEdgeIdTransformation().isEmpty());
        assertTrue("All collections should be empty by default", config.getSkipVertices().getById().isEmpty());
        assertTrue("All collections should be empty by default", config.getSkipVertices().getByLabel().isEmpty());
        assertTrue("All collections should be empty by default", config.getSkipEdges().getByLabel().isEmpty());
    }

    @Test
    public void testAutomaticYamlMapping() throws IOException {
        // Create a comprehensive YAML file to test automatic mapping
        File tempFile = File.createTempFile("test-auto-mapping", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Company: Organization\n");
            writer.write("edgeLabels:\n");
            writer.write("  WORKS_FOR: EMPLOYED_BY\n");
            writer.write("  KNOWS: CONNECTED_TO\n");
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"vertex_123\"\n");
            writer.write("    - \"vertex_456\"\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
            writer.write("    - \"Deprecated\"\n");
            writer.write("skipEdges:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TEMP_RELATIONSHIP\"\n");
            writer.write("    - \"DEBUG_LINK\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test vertex label mappings
        assertEquals(2, config.getVertexLabels().size());
        assertEquals("Individual", config.getVertexLabels().get("Person"));
        assertEquals("Organization", config.getVertexLabels().get("Company"));

        // Test edge label mappings
        assertEquals(2, config.getEdgeLabels().size());
        assertEquals("EMPLOYED_BY", config.getEdgeLabels().get("WORKS_FOR"));
        assertEquals("CONNECTED_TO", config.getEdgeLabels().get("KNOWS"));

        // Test skip vertex IDs
        assertEquals(2, config.getSkipVertices().getById().size());
        assertTrue(config.getSkipVertices().getById().contains("vertex_123"));
        assertTrue(config.getSkipVertices().getById().contains("vertex_456"));

        // Test skip vertex labels
        assertEquals(2, config.getSkipVertices().getByLabel().size());
        assertTrue(config.getSkipVertices().getByLabel().contains("TestData"));
        assertTrue(config.getSkipVertices().getByLabel().contains("Deprecated"));

        // Test skip edge labels
        assertEquals(2, config.getSkipEdges().getByLabel().size());
        assertTrue(config.getSkipEdges().getByLabel().contains("TEMP_RELATIONSHIP"));
        assertTrue(config.getSkipEdges().getByLabel().contains("DEBUG_LINK"));

    }

    @Test
    public void testPartialYamlConfiguration() throws IOException {
        // Test with only some sections present
        File tempFile = File.createTempFile("test-partial", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test that present sections work
        assertEquals(1, config.getVertexLabels().size());
        assertEquals("Individual", config.getVertexLabels().get("Person"));
        assertEquals(1, config.getSkipVertices().getByLabel().size());
        assertTrue(config.getSkipVertices().getByLabel().contains("TestData"));

        // Test that missing sections are empty but not null
        assertTrue(config.getEdgeLabels().isEmpty());
        assertTrue(config.getSkipVertices().getById().isEmpty());
        assertTrue(config.getSkipEdges().getByLabel().isEmpty());

    }

    @Test
    public void testPartialEmptyYamlConfiguration() throws IOException {
        // Test with only some sections present
        File tempFile = File.createTempFile("test-partial-empty", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        assertTrue(config.getVertexLabels().isEmpty());
        assertTrue(config.getEdgeLabels().isEmpty());
        assertTrue(config.getSkipVertices().getById().isEmpty());
        assertTrue(config.getSkipVertices().getByLabel().isEmpty());
        assertTrue(config.getSkipEdges().getByLabel().isEmpty());

    }

    @Test
    public void testEmptyYamlFile() throws IOException {
        // Test with empty YAML file
        File tempFile = File.createTempFile("test-empty", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            // Write empty file
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // All collections should be empty but not null
        assertNotNull(config.getVertexLabels());
        assertNotNull(config.getEdgeLabels());
        assertNotNull(config.getVertexIdTransformation());
        assertNotNull(config.getEdgeIdTransformation());
        assertNotNull(config.getSkipVertices().getById());
        assertNotNull(config.getSkipVertices().getByLabel());
        assertNotNull(config.getSkipEdges().getByLabel());

        assertTrue(config.getVertexLabels().isEmpty());
        assertTrue(config.getEdgeLabels().isEmpty());
        assertTrue(config.getVertexIdTransformation().isEmpty());
        assertTrue(config.getEdgeIdTransformation().isEmpty());
        assertTrue(config.getSkipVertices().getById().isEmpty());
        assertTrue(config.getSkipVertices().getByLabel().isEmpty());
        assertTrue(config.getSkipEdges().getByLabel().isEmpty());

        assertFalse("Should not have skip rules", config.hasSkipRules());
        assertFalse("Should not have ID transformations", config.hasIdTransformations());
    }

    @Test
    public void testNullFile() throws IOException {
        // Test with null file
        ConversionConfig config = ConversionConfig.fromFile(null);

        // All collections should be empty but not null
        assertNotNull(config.getVertexLabels());
        assertNotNull(config.getEdgeLabels());
        assertNotNull(config.getVertexIdTransformation());
        assertNotNull(config.getEdgeIdTransformation());
        assertNotNull(config.getSkipVertices().getById());
        assertNotNull(config.getSkipVertices().getByLabel());
        assertNotNull(config.getSkipEdges().getByLabel());

        assertTrue(config.getVertexLabels().isEmpty());
        assertTrue(config.getEdgeLabels().isEmpty());
        assertTrue(config.getVertexIdTransformation().isEmpty());
        assertTrue(config.getEdgeIdTransformation().isEmpty());
        assertTrue(config.getSkipVertices().getById().isEmpty());
        assertTrue(config.getSkipVertices().getByLabel().isEmpty());
        assertTrue(config.getSkipEdges().getByLabel().isEmpty());

        assertFalse("Should not have skip rules", config.hasSkipRules());
        assertFalse("Should not have ID transformations", config.hasIdTransformations());
    }

    @Test
    public void testMalformedYamlHandling() throws IOException {
        // Test with malformed YAML that should still parse partially
        File tempFile = File.createTempFile("test-malformed", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
            writer.write("    - TestData\n");  // No quotes, should still work
            writer.write("    - \"Deprecated\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Should still parse correctly
        assertEquals(1, config.getVertexLabels().size());
        assertEquals("Individual", config.getVertexLabels().get("Person"));
        assertEquals(2, config.getSkipVertices().getByLabel().size());
        assertTrue(config.getSkipVertices().getByLabel().contains("TestData"));
        assertTrue(config.getSkipVertices().getByLabel().contains("Deprecated"));
    }

    @Test
    public void testNonExistentFileHandling() throws IOException {
        // Test with non-existent file
        File nonExistentFile = new File("/path/that/does/not/exist.yaml");
        ConversionConfig config = ConversionConfig.fromFile(nonExistentFile);

        // Should return empty config
        assertNotNull(config);
        assertFalse(config.hasSkipRules());
        assertFalse(config.hasIdTransformations());
    }

    @Test
    public void testHasSkipRulesMethod() throws IOException {
        // Test hasSkipRules with no rules
        ConversionConfig emptyConfig = new ConversionConfig();
        assertFalse("Empty config should not have skip rules", emptyConfig.hasSkipRules());

        // Test hasSkipRules with vertex ID rules
        File tempFile1 = File.createTempFile("test-skip-id", ".yaml");
        tempFile1.deleteOnExit();
        try (FileWriter writer = new FileWriter(tempFile1)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
        }
        ConversionConfig configWithIdRules = ConversionConfig.fromFile(tempFile1);
        assertTrue("Config with vertex ID rules should have skip rules", configWithIdRules.hasSkipRules());

        // Test hasSkipRules with vertex label rules
        File tempFile2 = File.createTempFile("test-skip-label", ".yaml");
        tempFile2.deleteOnExit();
        try (FileWriter writer = new FileWriter(tempFile2)) {
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
        }
        ConversionConfig configWithLabelRules = ConversionConfig.fromFile(tempFile2);
        assertTrue("Config with vertex label rules should have skip rules", configWithLabelRules.hasSkipRules());

        // Test hasSkipRules with edge rules
        File tempFile3 = File.createTempFile("test-skip-edge", ".yaml");
        tempFile3.deleteOnExit();
        try (FileWriter writer = new FileWriter(tempFile3)) {
            writer.write("skipEdges:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TEMP\"\n");
        }
        ConversionConfig configWithEdgeRules = ConversionConfig.fromFile(tempFile3);
        assertTrue("Config with edge rules should have skip rules", configWithEdgeRules.hasSkipRules());
    }

    @Test
    public void testNestedObjectAccess() throws IOException {
        // Test direct access to nested objects
        File tempFile = File.createTempFile("test-nested", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"Test\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test direct access to nested objects
        assertNotNull(config.getSkipVertices());
        assertEquals(1, config.getSkipVertices().getById().size());
        assertEquals(1, config.getSkipVertices().getByLabel().size());
        assertTrue(config.getSkipVertices().getById().contains("123"));
        assertTrue(config.getSkipVertices().getByLabel().contains("Test"));
    }

    @Test
    public void testLombokAnnotations() throws IOException {
        // Test that Lombok annotations are working correctly
        File tempFile = File.createTempFile("test-lombok", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test that getters work (generated by Lombok)
        assertNotNull(config.getVertexLabels());
        assertNotNull(config.getSkipVertices());

        // Test that setters work (generated by Lombok)
        config.getVertexLabels().put("Company", "Organization");
        assertEquals("Organization", config.getVertexLabels().get("Company"));

        // Test toString method (generated by Lombok)
        String toString = config.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("ConversionConfig"));
    }

    @Test
    public void testConfigurationLoading() throws IOException {
        // Create a temporary YAML file
        File tempFile = File.createTempFile("test-config", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Company: Organization\n");
            writer.write("edgeLabels:\n");
            writer.write("  WORKS_FOR: EMPLOYED_BY\n");
            writer.write("  KNOWS: CONNECTED_TO\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        assertEquals(2, config.getVertexLabels().size());
        assertEquals(2, config.getEdgeLabels().size());

        assertEquals("Individual", config.getVertexLabels().get("Person"));
        assertEquals("Organization", config.getVertexLabels().get("Company"));
        assertEquals("EMPLOYED_BY", config.getEdgeLabels().get("WORKS_FOR"));
        assertEquals("CONNECTED_TO", config.getEdgeLabels().get("KNOWS"));
    }

    @Test
    public void testIdTransformationConfiguration() throws IOException {
        // Create a YAML file with ID transformation configurations
        File tempFile = File.createTempFile("test-id-transform", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexIdTransformation:\n");
            writer.write("  ~id: \"{_labels}_{_id}\"\n");
            writer.write("edgeIdTransformation:\n");
            writer.write("  ~id: \"{_type}_{_start}_{_end}\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test vertex ID transformation
        assertEquals(1, config.getVertexIdTransformation().size());
        assertEquals("{_labels}_{_id}", config.getVertexIdTransformation().get("~id"));

        // Test edge ID transformation
        assertEquals(1, config.getEdgeIdTransformation().size());
        assertEquals("{_type}_{_start}_{_end}", config.getEdgeIdTransformation().get("~id"));

        // Test hasIdTransformations method
        assertTrue(config.hasIdTransformations());
    }

    @Test
    public void testComplexIdTransformationConfiguration() throws IOException {
        // Create a YAML file with complex ID transformation configurations
        File tempFile = File.createTempFile("test-complex-id-transform", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Movie: Film\n");
            writer.write("edgeLabels:\n");
            writer.write("  ACTED_IN: PERFORMED_IN\n");
            writer.write("vertexIdTransformation:\n");
            writer.write("  ~id: \"{_labels}_{name}_{_id}\"\n");
            writer.write("edgeIdTransformation:\n");
            writer.write("  ~id: \"{_type}_{_start}_{_end}_{rating}\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test vertex ID transformation with property references
        assertEquals("{_labels}_{name}_{_id}", config.getVertexIdTransformation().get("~id"));

        // Test edge ID transformation with property references
        assertEquals("{_type}_{_start}_{_end}_{rating}", config.getEdgeIdTransformation().get("~id"));

        // Test that label mappings are also loaded correctly
        assertEquals(2, config.getVertexLabels().size());
        assertEquals("Individual", config.getVertexLabels().get("Person"));
        assertEquals("Film", config.getVertexLabels().get("Movie"));

        assertEquals(1, config.getEdgeLabels().size());
        assertEquals("PERFORMED_IN", config.getEdgeLabels().get("ACTED_IN"));
    }

    @Test
    public void testEmptyIdTransformationConfiguration() throws IOException {
        // Test with no ID transformation sections
        File tempFile = File.createTempFile("test-no-id-transform", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test that ID transformation maps are empty but not null
        assertNotNull(config.getVertexIdTransformation());
        assertTrue(config.getVertexIdTransformation().isEmpty());

        assertNotNull(config.getEdgeIdTransformation());
        assertTrue(config.getEdgeIdTransformation().isEmpty());

        // Test hasIdTransformations method
        assertFalse(config.hasIdTransformations());
    }

    @Test
    public void testIdTransformationWithGremlinFormat() throws IOException {
        // Create a YAML file with Gremlin format ID transformation configurations
        File tempFile = File.createTempFile("test-gremlin-id-transform", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexIdTransformation:\n");
            writer.write("  ~id: \"v_{_id}\"\n");
            writer.write("edgeIdTransformation:\n");
            writer.write("  ~id: \"e_{~label}_{~from}_{~to}\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test vertex ID transformation with Neo4j format
        assertEquals("v_{_id}", config.getVertexIdTransformation().get("~id"));

        // Test edge ID transformation with Gremlin format
        assertEquals("e_{~label}_{~from}_{~to}", config.getEdgeIdTransformation().get("~id"));
    }

    @Test
    public void testIdTransformationWithSpecificIds() throws IOException {
        // Test with specific ID mappings
        File tempFile = File.createTempFile("test-specific-ids", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexIdTransformation:\n");
            writer.write("  ~id: \"v_{_id}\"\n");
            writer.write("  123: \"custom_123\"\n");
            writer.write("  456: \"custom_456\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test that both template and specific ID mappings are loaded
        assertEquals(3, config.getVertexIdTransformation().size());
        assertEquals("v_{_id}", config.getVertexIdTransformation().get("~id"));
        assertEquals("custom_123", config.getVertexIdTransformation().get("123"));
        assertEquals("custom_456", config.getVertexIdTransformation().get("456"));
    }

    @Test
    public void testIdTransformationWithAllFeatures() throws IOException {
        // Create a YAML file with all features combined
        File tempFile = File.createTempFile("test-all-features", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertexLabels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Movie: Film\n");
            writer.write("edgeLabels:\n");
            writer.write("  ACTED_IN: PERFORMED_IN\n");
            writer.write("  DIRECTED: CREATED\n");
            writer.write("vertexIdTransformation:\n");
            writer.write("  ~id: \"{_labels}_{name}_{_id}\"\n");
            writer.write("edgeIdTransformation:\n");
            writer.write("  ~id: \"{_type}_{_start}_{_end}\"\n");
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
            writer.write("skipEdges:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TEMP_RELATIONSHIP\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);

        // Test that all configurations are loaded correctly
        assertEquals(2, config.getVertexLabels().size());
        assertEquals("Individual", config.getVertexLabels().get("Person"));

        assertEquals(2, config.getEdgeLabels().size());
        assertEquals("PERFORMED_IN", config.getEdgeLabels().get("ACTED_IN"));

        assertEquals("{_labels}_{name}_{_id}", config.getVertexIdTransformation().get("~id"));
        assertEquals("{_type}_{_start}_{_end}", config.getEdgeIdTransformation().get("~id"));

        assertEquals(1, config.getSkipVertices().getById().size());
        assertTrue(config.getSkipVertices().getById().contains("123"));

        assertEquals(1, config.getSkipVertices().getByLabel().size());
        assertTrue(config.getSkipVertices().getByLabel().contains("TestData"));

        assertEquals(1, config.getSkipEdges().getByLabel().size());
        assertTrue(config.getSkipEdges().getByLabel().contains("TEMP_RELATIONSHIP"));

        // Test both helper methods
        assertTrue(config.hasSkipRules());
        assertTrue(config.hasIdTransformations());
    }
}
