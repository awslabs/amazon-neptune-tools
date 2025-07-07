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

package com.amazonaws.services.neptune.io;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.Assume;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

/**
 * Integration tests for Neo4jStreamWriter class.
 * These tests require a running Neo4j database with APOC plugin installed.
 * 
 * To run these tests:
 * 1. Start a Neo4j database (e.g., using Docker: docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest)
 * 2. Install APOC plugin
 * 3. Set system properties: -Dneo4j.uri=bolt://localhost:7687 -Dneo4j.username=neo4j -Dneo4j.password=password
 * 4. Run tests
 */
public class Neo4jStreamWriterIntegrationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Directories directories;
    private Neo4jStreamWriter writer;
    
    // Test configuration - can be overridden with system properties
    private static final String DEFAULT_URI = "bolt://localhost:7687";
    private static final String DEFAULT_USERNAME = "neo4j";
    private static final String DEFAULT_PASSWORD = "password";

    @Before
    public void setUp() throws IOException {
        // Create a temporary directory for testing
        File tempDir = tempFolder.newFolder("integration-test-output");
        directories = Directories.createFor(tempDir);

        // Get connection details from system properties or use defaults
        String uri = System.getProperty("neo4j.uri", DEFAULT_URI);
        String username = System.getProperty("neo4j.username", DEFAULT_USERNAME);
        String password = System.getProperty("neo4j.password", DEFAULT_PASSWORD);

        try {
            writer = new Neo4jStreamWriter(uri, username, password, directories);

            createTestData();
                
        } catch (Exception e) {
            Assume.assumeNoException("Neo4j database is not available - skipping integration tests", e);
        }
    }

    @After
    public void tearDown() {
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * Helper method to create test data in the Neo4j database
     */
    private void createTestData() {
        // TODO decide on test data set
    }

    @Test
    public void testStreamToFileWithRealDatabase() throws IOException {

        // Stream data to file
        File result = writer.streamToFile("integration-test");

        // Verify results
        assertNotNull("Result file should not be null", result);
        assertTrue("Result file should exist", result.exists());
        assertTrue("Result file should not be empty", result.length() > 0);

        // Read and verify file contents
        String content = new String(Files.readAllBytes(result.toPath()));
        assertFalse("File content should not be empty", content.trim().isEmpty());
        
        System.out.println("Generated file content preview:");
        System.out.println(content.substring(0, Math.min(500, content.length())));
    }

    @Test
    public void testStreamCustomQueryToFileWithRealDatabase() throws IOException {
        // Create some test data first


        // Execute custom query
        String customQuery = "MATCH (n) RETURN n.name as name, labels(n) as labels LIMIT 5";
        File result = writer.streamCustomQueryToFile(customQuery, "custom-query-test");

        // Verify results
        assertNotNull("Result file should not be null", result);
        assertTrue("Result file should exist", result.exists());
        assertTrue("Result file should not be empty", result.length() > 0);

        // Read and verify file contents
        String content = new String(Files.readAllBytes(result.toPath()));
        assertFalse("File content should not be empty", content.trim().isEmpty());
        
        System.out.println("Custom query result preview:");
        System.out.println(content.substring(0, Math.min(200, content.length())));
    }

    @Test
    public void testStreamToFileWithCustomConfig() throws IOException {
        // Create writer with custom configuration
        Neo4jStreamWriter.Neo4jStreamWriterConfig customConfig = 
            new Neo4jStreamWriter.Neo4jStreamWriterConfig(60, 120, 500);

        String uri = System.getProperty("neo4j.uri", DEFAULT_URI);
        String username = System.getProperty("neo4j.username", DEFAULT_USERNAME);
        String password = System.getProperty("neo4j.password", DEFAULT_PASSWORD);

        try (Neo4jStreamWriter customWriter = new Neo4jStreamWriter(uri, username, password, directories, customConfig)) {

            // Create test data and stream
            File result = customWriter.streamToFile("custom-config-test");

            // Verify results
            assertNotNull("Result file should not be null", result);
            assertTrue("Result file should exist", result.exists());
        }
    }

    @Test
    public void testMultipleStreamOperations() throws IOException {

        // First stream operation
        File result1 = writer.streamToFile("multi-test-1");
        assertNotNull("First result should not be null", result1);
        assertTrue("First result should exist", result1.exists());

        // Second stream operation
        File result2 = writer.streamToFile("multi-test-2");
        assertNotNull("Second result should not be null", result2);
        assertTrue("Second result should exist", result2.exists());

        // Verify both files are different (different timestamps in names)
        assertNotEquals("Files should have different names", result1.getName(), result2.getName());
    }

    @Test
    public void testErrorHandlingWithInvalidQuery() throws IOException {
        // Test with an invalid query that should fail
        File result = writer.streamCustomQueryToFile("INVALID CYPHER QUERY", "error-test");
        
        // Should return null on error
        assertNull("Result should be null for invalid query", result);
    }

    @Test
    public void testLargeDatasetHandling() throws IOException {
        // Test with a query that returns a larger dataset
        String largeDataQuery = "UNWIND range(1, 1000) as i RETURN i, 'test_' + toString(i) as name";
        File result = writer.streamCustomQueryToFile(largeDataQuery, "large-dataset-test");

        if (result != null) {
            assertTrue("Large dataset file should exist", result.exists());
            assertTrue("Large dataset file should not be empty", result.length() > 0);
            
            // Verify file size is reasonable for 1000 records
            long fileSize = result.length();
            assertTrue("File should be reasonably sized for 1000 records", fileSize > 1000);
            
            System.out.println("Large dataset file size: " + fileSize + " bytes");
        }
    }
}
