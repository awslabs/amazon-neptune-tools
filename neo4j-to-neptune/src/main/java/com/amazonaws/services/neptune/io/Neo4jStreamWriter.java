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

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A writer class that streams data from Neo4j database to CSV files.
 * This class handles the connection to Neo4j, executes export queries,
 * and writes the results to temporary files for further processing.
 */
public class Neo4jStreamWriter implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(Neo4jStreamWriter.class.getName());
    private static final String TEMP_FILE = "neo4j-stream-data";
    
    private final Directories directories;
    private final Driver driver;
    private final Neo4jStreamWriterConfig config;

    /**
     * Creates a new Neo4jStreamWriter with default configuration.
     *
     * @param uri The Neo4j database URI
     * @param username The database username
     * @param password The database password
     * @param directories The directories helper for file management
     * @throws IllegalArgumentException if any parameter is null or invalid
     */
    public Neo4jStreamWriter(String uri, String username, String password, Directories directories) {
        this(uri, username, password, directories, Neo4jStreamWriterConfig.defaultConfig());
    }

    /**
     * Creates a new Neo4jStreamWriter with custom configuration.
     *
     * @param uri The Neo4j database URI
     * @param username The database username
     * @param password The database password
     * @param directories The directories helper for file management
     * @param config The configuration for the Neo4j driver
     * @throws IllegalArgumentException if any parameter is null or invalid
     */
    public Neo4jStreamWriter(String uri, String username, String password, Directories directories, Neo4jStreamWriterConfig config) {
        validateInputs(uri, username, password, directories, config);
        
        this.directories = directories;
        this.config = config;
        
        try {
            this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password),
                Config.builder()
                    .withConnectionTimeout(config.getConnectionTimeoutSeconds(), TimeUnit.SECONDS)
                    .withMaxConnectionLifetime(config.getMaxConnectionLifetimeMinutes(), TimeUnit.MINUTES)
                    .build());

            driver.verifyConnectivity();

            LOGGER.info("Successfully connected to Neo4j database at: " + uri);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to connect to Neo4j database at: " + uri, e);
            throw new RuntimeException("Failed to connect to Neo4j database", e);
        }
    }

    /**
     * Issues a query to the database to stream all data back inside the "data" column in CSV format
     * and writes it to a temporary file for conversion.
     *
     * @return The file containing the exported data, or null if the operation failed
     */
    public File streamToFile() {
        return streamToFile(TEMP_FILE);
    }

    /**
     * Issues a query to the database to stream all data back inside the "data" column in CSV format
     * and writes it to a temporary file for conversion.
     *
     * @param baseFileName The base name for the output file
     * @return The file containing the exported data, or null if the operation failed
     */
    public File streamToFile(String baseFileName) {
        if (baseFileName == null || baseFileName.trim().isEmpty()) {
            throw new IllegalArgumentException("Base file name cannot be null or empty");
        }

        Path tempFilePath = directories.createFilePath(baseFileName, "temp");
        File file = tempFilePath.toFile();
        
        LOGGER.info("Starting data export to file: " + file.getAbsolutePath());

        try (BufferedWriter writer = Files.newBufferedWriter(tempFilePath, StandardCharsets.UTF_8);
             Session session = driver.session()) {

            String exportQuery = buildExportQuery();
            LOGGER.fine("Executing query: " + exportQuery);

            Result result = session.run(exportQuery);
            long recordCount = 0;
            long lineCount = 0;

            while (result.hasNext()) {
                Record record = result.next();
                recordCount++;
                
                Map<String, Object> recordMap = record.asMap();
                logRecordInfo(recordMap, recordCount);
                
                Object dataObject = recordMap.get("data");
                if (dataObject != null) {
                    lineCount += processDataObject(dataObject, writer);
                }
            }

            LOGGER.info(String.format("Successfully exported %d records (%d lines) to file: %s", 
                recordCount, lineCount, file.getAbsolutePath()));
            return file;

        } catch (Neo4jException e) {
            LOGGER.log(Level.SEVERE, "Neo4j database error during export", e);
            return null;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IO error while writing to file: " + file.getAbsolutePath(), e);
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error during data export", e);
            return null;
        }
    }

    /**
     * Executes a custom Cypher query and streams the results to a file.
     *
     * @param query The Cypher query to execute
     * @param baseFileName The base name for the output file
     * @return The file containing the exported data, or null if the operation failed
     */
    public File streamCustomQueryToFile(String query, String baseFileName) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }
        if (baseFileName == null || baseFileName.trim().isEmpty()) {
            throw new IllegalArgumentException("Base file name cannot be null or empty");
        }

        Path tempFilePath = directories.createFilePath(baseFileName, "custom");
        File file = tempFilePath.toFile();

        LOGGER.info("Starting custom query export to file: " + file.getAbsolutePath());

        try (BufferedWriter writer = Files.newBufferedWriter(tempFilePath, StandardCharsets.UTF_8);
             Session session = driver.session()) {

            LOGGER.fine("Executing custom query: " + query);

            Result result = session.run(query);
            long recordCount = 0;

            while (result.hasNext()) {
                Record record = result.next();
                recordCount++;

                // Write the entire record as a line
                writer.write(record.toString());
                writer.newLine();
            }
            writer.flush();
            
            LOGGER.info(String.format("Successfully exported %d records from custom query to file: %s",
                recordCount, file.getAbsolutePath()));
            return file;

        } catch (Neo4jException e) {
            LOGGER.log(Level.SEVERE, "Neo4j database error during custom query export", e);
            return null;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "IO error while writing to file: " + file.getAbsolutePath(), e);
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error during custom query export", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (driver != null) {
            try {
                driver.close();
                LOGGER.info("Neo4j driver closed successfully");
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing Neo4j driver", e);
            }
        }
    }

    private void validateInputs(String uri, String username, String password, Directories directories, Neo4jStreamWriterConfig config) {
        if (uri == null || uri.trim().isEmpty()) {
            throw new IllegalArgumentException("URI cannot be null or empty");
        }
        if (username == null) {
            throw new IllegalArgumentException("Username cannot be null");
        }
        if (password == null) {
            throw new IllegalArgumentException("Password cannot be null");
        }
        if (directories == null) {
            throw new IllegalArgumentException("Directories cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }
    }

    private String buildExportQuery() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CALL apoc.export.csv.all(null, {stream:true");
        
        if (config.getBatchSize() > 0) {
            queryBuilder.append(", batchSize:").append(config.getBatchSize());
        }
        
        queryBuilder.append("})\n")
                   .append("YIELD file, nodes, relationships, properties, data\n")
                   .append("RETURN file, nodes, relationships, properties, data");
        
        return queryBuilder.toString();
    }

    private void logRecordInfo(Map<String, Object> recordMap, long recordCount) {
        if (LOGGER.isLoggable(Level.FINE)) {
            Object nodes = recordMap.get("nodes");
            Object relationships = recordMap.get("relationships");
            Object properties = recordMap.get("properties");
            
            LOGGER.fine(String.format("Processing record %d - Nodes: %s, Relationships: %s, Properties: %s", 
                recordCount, nodes, relationships, properties));
        }
    }

    private long processDataObject(Object dataObject, BufferedWriter writer) throws IOException {
        String dataString = dataObject.toString();
        
        // Handle different line separator patterns
        String[] lines = dataString.split("\\r?\\n|%n");
        long lineCount = 0;
        
        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                writer.write(line);
                writer.newLine();
                lineCount++;
            }
        }
        writer.flush();
        
        return lineCount;
    }

    /**
     * Configuration class for Neo4jStreamWriter
     */
    public static class Neo4jStreamWriterConfig {
        private final int connectionTimeoutSeconds;
        private final int maxConnectionLifetimeMinutes;
        private final int batchSize;

        public Neo4jStreamWriterConfig(int connectionTimeoutSeconds, int maxConnectionLifetimeMinutes, int batchSize) {
            this.connectionTimeoutSeconds = connectionTimeoutSeconds;
            this.maxConnectionLifetimeMinutes = maxConnectionLifetimeMinutes;
            this.batchSize = batchSize;
        }

        public static Neo4jStreamWriterConfig defaultConfig() {
            return new Neo4jStreamWriterConfig(30, 60, 1000);
        }

        public int getConnectionTimeoutSeconds() {
            return connectionTimeoutSeconds;
        }

        public int getMaxConnectionLifetimeMinutes() {
            return maxConnectionLifetimeMinutes;
        }

        public int getBatchSize() {
            return batchSize;
        }
    }
}
