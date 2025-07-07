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

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class ConversionConfigTest {

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
        assertNotNull(config.getSkipVertices().getById());
        assertNotNull(config.getSkipVertices().getByLabel());
        assertNotNull(config.getSkipEdges().getByLabel());
        
        assertTrue(config.getVertexLabels().isEmpty());
        assertTrue(config.getEdgeLabels().isEmpty());
        assertTrue(config.getSkipVertices().getById().isEmpty());
        assertTrue(config.getSkipVertices().getByLabel().isEmpty());
        assertTrue(config.getSkipEdges().getByLabel().isEmpty());

    }

    @Test
    public void testNullFile() throws IOException {
        // Test with null file
        ConversionConfig config = ConversionConfig.fromFile(null);
        
        // All collections should be empty but not null
        assertNotNull(config.getVertexLabels());
        assertNotNull(config.getEdgeLabels());
        assertNotNull(config.getSkipVertices().getById());
        assertNotNull(config.getSkipVertices().getByLabel());
        assertNotNull(config.getSkipEdges().getByLabel());
        
        assertTrue(config.getVertexLabels().isEmpty());
        assertTrue(config.getEdgeLabels().isEmpty());
        assertTrue(config.getSkipVertices().getById().isEmpty());
        assertTrue(config.getSkipVertices().getByLabel().isEmpty());
        assertTrue(config.getSkipEdges().getByLabel().isEmpty());

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
}
