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

package com.amazonaws.services.neptune.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Unit tests for Neo4jStreamWriter class focusing on validation and configuration.
 * These tests don't require a Neo4j database connection.
 */
public class Neo4jStreamWriterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Directories directories;

    @Before
    public void setUp() throws IOException {
        // Create a temporary directory for testing
        File tempDir = tempFolder.newFolder("test-output");
        directories = Directories.createFor(tempDir);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullUri() {
        new Neo4jStreamWriter(null, "neo4j", "password", directories);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithEmptyUri() {
        new Neo4jStreamWriter("", "neo4j", "password", directories);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithWhitespaceUri() {
        new Neo4jStreamWriter("   ", "neo4j", "password", directories);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullUsername() {
        new Neo4jStreamWriter("bolt://localhost:7687", null, "password", directories);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullPassword() {
        new Neo4jStreamWriter("bolt://localhost:7687", "neo4j", null, directories);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullDirectories() {
        new Neo4jStreamWriter("bolt://localhost:7687", "neo4j", "password", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullConfig() {
        new Neo4jStreamWriter("bolt://localhost:7687", "neo4j", "password", directories, null);
    }

    @Test
    public void testConstructorValidationPasses() {
        // This will fail with connection error, but validation should pass
        try  {
            new Neo4jStreamWriter("bolt://localhost:7687", "neo4j", "password", directories);
            fail("Should have thrown RuntimeException due to connection failure");
        } catch (RuntimeException e) {
            // Expected - connection will fail, but validation passed
            assertTrue("Should fail due to connection, not validation", 
                e.getMessage().contains("Failed to connect to Neo4j database"));
        }
    }

    @Test
    public void testConfigurationDefaults() {
        Neo4jStreamWriter.Neo4jStreamWriterConfig config = 
            Neo4jStreamWriter.Neo4jStreamWriterConfig.defaultConfig();

        assertEquals("Default connection timeout should be 30 seconds", 
            30, config.getConnectionTimeoutSeconds());
        assertEquals("Default max connection lifetime should be 60 minutes", 
            60, config.getMaxConnectionLifetimeMinutes());
        assertEquals("Default batch size should be 1000", 
            1000, config.getBatchSize());
    }

    @Test
    public void testCustomConfiguration() {
        Neo4jStreamWriter.Neo4jStreamWriterConfig config = 
            new Neo4jStreamWriter.Neo4jStreamWriterConfig(60, 120, 500);

        assertEquals("Custom connection timeout should be 60 seconds", 
            60, config.getConnectionTimeoutSeconds());
        assertEquals("Custom max connection lifetime should be 120 minutes", 
            120, config.getMaxConnectionLifetimeMinutes());
        assertEquals("Custom batch size should be 500", 
            500, config.getBatchSize());
    }

    @Test
    public void testConfigurationWithZeroValues() {
        Neo4jStreamWriter.Neo4jStreamWriterConfig config = 
            new Neo4jStreamWriter.Neo4jStreamWriterConfig(0, 0, 0);

        assertEquals("Zero connection timeout should be allowed", 
            0, config.getConnectionTimeoutSeconds());
        assertEquals("Zero max connection lifetime should be allowed", 
            0, config.getMaxConnectionLifetimeMinutes());
        assertEquals("Zero batch size should be allowed", 
            0, config.getBatchSize());
    }

    @Test
    public void testConfigurationWithNegativeValues() {
        Neo4jStreamWriter.Neo4jStreamWriterConfig config = 
            new Neo4jStreamWriter.Neo4jStreamWriterConfig(-1, -1, -1);

        assertEquals("Negative connection timeout should be allowed", 
            -1, config.getConnectionTimeoutSeconds());
        assertEquals("Negative max connection lifetime should be allowed", 
            -1, config.getMaxConnectionLifetimeMinutes());
        assertEquals("Negative batch size should be allowed", 
            -1, config.getBatchSize());
    }

    @Test
    public void testDirectoriesIntegration() throws IOException {
        // Test that the directories object works correctly with the writer
        assertNotNull("Directories should not be null", directories);
        assertNotNull("Output directory should not be null", directories.outputDirectory());
        assertTrue("Output directory should exist", directories.outputDirectory().toFile().exists());
        
        // Test file path creation
        java.nio.file.Path filePath = directories.createFilePath("test-file", "temp");
        assertNotNull("File path should not be null", filePath);
        assertTrue("File path should contain test-file", filePath.toString().contains("test-file"));
        assertTrue("File path should contain temp", filePath.toString().contains("temp"));
        assertTrue("File path should end with .csv", filePath.toString().endsWith(".csv"));
    }

    @Test
    public void testFilePathGeneration() {
        // Test different file path scenarios
        java.nio.file.Path path1 = directories.createFilePath("export");
        java.nio.file.Path path2 = directories.createFilePath("export", "1");
        java.nio.file.Path path3 = directories.createFilePath("export", "temp");

        assertNotEquals("Different paths should be generated", path1, path2);
        assertNotEquals("Different paths should be generated", path1, path3);
        assertNotEquals("Different paths should be generated", path2, path3);

        assertTrue("All paths should end with .csv", path1.toString().endsWith(".csv"));
        assertTrue("All paths should end with .csv", path2.toString().endsWith(".csv"));
        assertTrue("All paths should end with .csv", path3.toString().endsWith(".csv"));
    }

    @Test
    public void testEmptyStringValidation() {
        // Test various empty string scenarios
        String[] emptyStrings = {"", "   ", "\t", "\n", "\r\n", null};
        
        for (String emptyString : emptyStrings) {
            try {
                new Neo4jStreamWriter(emptyString, "neo4j", "password", directories);
                fail("Should have thrown IllegalArgumentException for empty URI: '" + emptyString + "'");
            } catch (IllegalArgumentException e) {
                // Expected
                assertTrue("Error message should mention URI", 
                    e.getMessage().toLowerCase().contains("uri"));
            }
        }
    }

    @Test
    public void testValidUriFormats() {
        // Test that various valid URI formats pass validation (will fail on connection)
        String[] validUris = {
            "bolt://localhost:7687",
            "bolt+s://localhost:7687",
            "bolt+ssc://localhost:7687",
            "neo4j://localhost:7687",
            "neo4j+s://localhost:7687",
            "neo4j+ssc://localhost:7687"
        };

        for (String uri : validUris) {
            try {
                new Neo4jStreamWriter(uri, "neo4j", "password", directories);
                fail("Should have thrown RuntimeException due to connection failure for URI: " + uri);
            } catch (RuntimeException e) {
                // Expected - connection will fail, but validation should pass
                assertTrue("Should fail due to connection, not validation for URI: " + uri, 
                    e.getMessage().contains("Failed to connect to Neo4j database"));
            }
        }
    }

    @Test
    public void testUsernameAndPasswordValidation() {
        // Test that empty username and password are allowed (some Neo4j setups don't require auth)
        try {
            new Neo4jStreamWriter("bolt://localhost:7687", "", "", directories);
            fail("Should have thrown RuntimeException due to connection failure");
        } catch (RuntimeException e) {
            // Expected - connection will fail, but validation should pass
            assertTrue("Should fail due to connection, not validation", 
                e.getMessage().contains("Failed to connect to Neo4j database"));
        }
    }
}
