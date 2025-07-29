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

import com.amazonaws.services.neptune.TestDataProvider;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class BulkLoadConfigTest {

    @Test
    public void testCompleteYamlConfiguration() throws IOException {
        // Create a complete YAML file with all possible configurations
        File tempFile = File.createTempFile("test-complete", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("bucket-name: \"" + TestDataProvider.BUCKET + "\"\n");
            writer.write("s3-prefix: \"" + TestDataProvider.S3_PREFIX + "\"\n");
            writer.write("neptune-endpoint: \"" + TestDataProvider.NEPTUNE_ENDPOINT + "\"\n");
            writer.write("iam-role-arn: \"" + TestDataProvider.IAM_ROLE_ARN + "\"\n");
            writer.write("parallelism: \"" + TestDataProvider.BULK_LOAD_PARALLELISM_LOW + "\"\n");
            writer.write("monitor: " + TestDataProvider.BULK_LOAD_MONITOR_TRUE+ "\n");
        }

        BulkLoadConfig config = BulkLoadConfig.fromFile(tempFile);

        // Test all fields are loaded correctly
        assertEquals(TestDataProvider.BUCKET, config.getBucketName());
        assertEquals(TestDataProvider.S3_PREFIX, config.getS3Prefix());
        assertEquals(TestDataProvider.NEPTUNE_ENDPOINT, config.getNeptuneEndpoint());
        assertEquals(TestDataProvider.IAM_ROLE_ARN, config.getIamRoleArn());
        assertEquals(TestDataProvider.BULK_LOAD_PARALLELISM_LOW, config.getParallelism());
        assertTrue(config.isMonitor());
    }

    @Test
    public void testDefaultYamlConfiguration() throws IOException {
        // Test with only required fields
        File tempFile = File.createTempFile("test-default", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("bucket-name: \"" + TestDataProvider.BUCKET + "\"\n");
            writer.write("neptune-endpoint: \"" + TestDataProvider.NEPTUNE_ENDPOINT + "\"\n");
            writer.write("iam-role-arn: \"" + TestDataProvider.IAM_ROLE_ARN + "\"\n");
        }

        BulkLoadConfig config = BulkLoadConfig.fromFile(tempFile);

        // Test required fields
        assertEquals(TestDataProvider.BUCKET, config.getBucketName());
        assertEquals(TestDataProvider.NEPTUNE_ENDPOINT, config.getNeptuneEndpoint());
        assertEquals(TestDataProvider.IAM_ROLE_ARN, config.getIamRoleArn());

        // Test default values for optional fields
        assertEquals("", config.getS3Prefix());
        assertEquals("OVERSUBSCRIBE", config.getParallelism());
        assertFalse(config.isMonitor()); // boolean default is false
    }

    @Test
    public void testEmptyYamlFile() throws IOException {
        // Test with empty YAML file
        File tempFile = File.createTempFile("test-empty", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            // Write empty file
        }

        BulkLoadConfig config = BulkLoadConfig.fromFile(tempFile);

        // CLI missing parameters validation will kick in all fields should be null values
        assertNull(config.getBucketName());
        assertNull(config.getS3Prefix());
        assertNull(config.getNeptuneEndpoint());
        assertNull(config.getIamRoleArn());
        assertNull(config.getParallelism());
        assertFalse(config.isMonitor()); // boolean default is false
    }

    @Test
    public void testNullFile() throws IOException {
        // Test with null file
        BulkLoadConfig config = BulkLoadConfig.fromFile(null);

        // CLI missing parameters validation will kick in all fields should be null values
        assertNull(config.getBucketName());
        assertNull(config.getS3Prefix());
        assertNull(config.getNeptuneEndpoint());
        assertNull(config.getIamRoleArn());
        assertNull(config.getParallelism());
        assertFalse(config.isMonitor()); // boolean default is false
    }

    @Test
    public void testBooleanParsing() throws IOException {
        // Test boolean parsing from different formats
        File tempFile = File.createTempFile("test-boolean", ".yaml");
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("bucket-name: \"" + TestDataProvider.BUCKET + "\"\n");
            writer.write("neptune-endpoint: \"" + TestDataProvider.NEPTUNE_ENDPOINT + "\"\n");
            writer.write("iam-role-arn: \"" + TestDataProvider.IAM_ROLE_ARN + "\"\n");
            writer.write("monitor: " + TestDataProvider.BULK_LOAD_MONITOR_FALSE + "\n");
        }

        BulkLoadConfig config = BulkLoadConfig.fromFile(tempFile);
        assertFalse(config.isMonitor());

        // Test boolean true
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("bucket-name: \"" + TestDataProvider.BUCKET + "\"\n");
            writer.write("neptune-endpoint: \"" + TestDataProvider.NEPTUNE_ENDPOINT + "\"\n");
            writer.write("iam-role-arn: \"" + TestDataProvider.IAM_ROLE_ARN + "\"\n");
            writer.write("monitor: " + TestDataProvider.BULK_LOAD_MONITOR_TRUE + "\n");
        }

        config = BulkLoadConfig.fromFile(tempFile);
        assertTrue(config.isMonitor());
    }

    @Test
    public void testFluentSetters() {
        // Test the fluent setter methods
        BulkLoadConfig config = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        assertEquals(TestDataProvider.BUCKET, config.getBucketName());
        assertEquals(TestDataProvider.NEPTUNE_ENDPOINT, config.getNeptuneEndpoint());
        assertEquals(TestDataProvider.IAM_ROLE_ARN, config.getIamRoleArn());
    }

    @Test
    public void testFluentSettersWithNullValues() {
        // Test that null values don't overwrite existing values
        BulkLoadConfig config = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        // Try to set null values
        config.withBucketName(null)
              .withNeptuneEndpoint(null)
              .withIamRoleArn(null);

        // Values should remain unchanged
        assertEquals(TestDataProvider.BUCKET, config.getBucketName());
        assertEquals(TestDataProvider.NEPTUNE_ENDPOINT, config.getNeptuneEndpoint());
        assertEquals(TestDataProvider.IAM_ROLE_ARN, config.getIamRoleArn());
    }

    @Test
    public void testFluentSettersWithEmptyValues() {
        // Test that empty strings don't overwrite existing values
        BulkLoadConfig config = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        // Try to set empty values
        config.withBucketName("")
              .withNeptuneEndpoint("")
              .withIamRoleArn("");

        // Values should remain unchanged
        assertEquals(TestDataProvider.BUCKET, config.getBucketName());
        assertEquals(TestDataProvider.NEPTUNE_ENDPOINT, config.getNeptuneEndpoint());
        assertEquals(TestDataProvider.IAM_ROLE_ARN, config.getIamRoleArn());
    }

    @Test
    public void testValidationSuccess() {
        // Test validation with valid config
        BulkLoadConfig validConfig = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        try {
            BulkLoadConfig.validateBulkLoadConfigFile(validConfig);
        } catch (Exception e) {
            fail("Valid config should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testValidationMissingAllRequiredFields() {
        // Test validation with all required fields missing
        BulkLoadConfig emptyConfig = new BulkLoadConfig();

        try {
            BulkLoadConfig.validateBulkLoadConfigFile(emptyConfig);
            fail("Should throw exception for missing required fields");
        } catch (IllegalArgumentException e) {
            // Verify that the error message contains all missing fields
            String errorMsg = e.getMessage();
            assertTrue("Error message should mention Neptune endpoint",
                errorMsg.contains("Neptune endpoint"));
            assertTrue("Error message should mention S3 bucket name",
                errorMsg.contains("S3 bucket name"));
            assertTrue("Error message should mention IAM role ARN",
                errorMsg.contains("IAM role ARN"));
        }
    }

    @Test
    public void testValidationMissingBucketName() {
        // Test validation with only bucket name missing
        BulkLoadConfig missingBucket = new BulkLoadConfig()
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        try {
            BulkLoadConfig.validateBulkLoadConfigFile(missingBucket);
            fail("Should throw exception for missing bucket name");
        } catch (IllegalArgumentException e) {
            assertTrue("Error message should mention S3 bucket name",
                e.getMessage().contains("S3 bucket name"));
            assertFalse("Error message should not mention Neptune endpoint",
                e.getMessage().contains("Neptune endpoint"));
            assertFalse("Error message should not mention IAM role ARN",
                e.getMessage().contains("IAM role ARN"));
        }
    }

    @Test
    public void testValidationMissingNeptuneEndpoint() {
        // Test validation with only Neptune endpoint missing
        BulkLoadConfig missingEndpoint = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        try {
            BulkLoadConfig.validateBulkLoadConfigFile(missingEndpoint);
            fail("Should throw exception for missing Neptune endpoint");
        } catch (IllegalArgumentException e) {
            assertTrue("Error message should mention Neptune endpoint",
                e.getMessage().contains("Neptune endpoint"));
            assertFalse("Error message should not mention S3 bucket name",
                e.getMessage().contains("S3 bucket name"));
            assertFalse("Error message should not mention IAM role ARN",
                e.getMessage().contains("IAM role ARN"));
        }
    }

    @Test
    public void testValidationMissingIAMRoleArn() {
        // Test validation with only IAM role ARN missing
        BulkLoadConfig missingRole = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT);

        try {
            BulkLoadConfig.validateBulkLoadConfigFile(missingRole);
            fail("Should throw exception for missing IAM role ARN");
        } catch (IllegalArgumentException e) {
            assertTrue("Error message should mention IAM role ARN",
                e.getMessage().contains("IAM role ARN"));
            assertFalse("Error message should not mention Neptune endpoint",
                e.getMessage().contains("Neptune endpoint"));
            assertFalse("Error message should not mention S3 bucket name",
                e.getMessage().contains("S3 bucket name"));
        }
    }

    @Test
    public void testValidationInvalidParallelism() {
        // Test validation with invalid parallelism value
        BulkLoadConfig invalidParallelism = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        // Set invalid parallelism
        invalidParallelism.setParallelism("INVALID_VALUE");

        try {
            BulkLoadConfig.validateBulkLoadConfigFile(invalidParallelism);
            fail("Should throw exception for invalid parallelism");
        } catch (IllegalArgumentException e) {
            assertTrue("Error message should mention valid parallelism options",
                e.getMessage().contains("Parallelism must be one of"));
        }
    }

    @Test
    public void testValidationNullParallelism() {
        // Test validation with null parallelism
        BulkLoadConfig nullParallelism = new BulkLoadConfig()
            .withBucketName(TestDataProvider.BUCKET)
            .withNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT)
            .withIamRoleArn(TestDataProvider.IAM_ROLE_ARN);

        // Set null parallelism
        nullParallelism.setParallelism(null);

        // This should not throw an exception since we now allow null parallelism
        try {
            BulkLoadConfig.validateBulkLoadConfigFile(nullParallelism);
        } catch (Exception e) {
            fail("Should not throw exception for null parallelism: " + e.getMessage());
        }
    }

    @Test
    public void testLombokAnnotations() {
        // Test that Lombok annotations are working correctly
        BulkLoadConfig config = new BulkLoadConfig();

        // Test setters (generated by Lombok)
        config.setBucketName(TestDataProvider.BUCKET);
        config.setS3Prefix(TestDataProvider.S3_PREFIX);
        config.setNeptuneEndpoint(TestDataProvider.NEPTUNE_ENDPOINT);
        config.setIamRoleArn(TestDataProvider.IAM_ROLE_ARN);
        config.setParallelism(TestDataProvider.BULK_LOAD_PARALLELISM_HIGH);
        config.setMonitor(TestDataProvider.BULK_LOAD_MONITOR_FALSE);

        // Test getters (generated by Lombok)
        assertEquals(TestDataProvider.BUCKET, config.getBucketName());
        assertEquals(TestDataProvider.S3_PREFIX, config.getS3Prefix());
        assertEquals(TestDataProvider.NEPTUNE_ENDPOINT, config.getNeptuneEndpoint());
        assertEquals(TestDataProvider.IAM_ROLE_ARN, config.getIamRoleArn());
        assertEquals(TestDataProvider.BULK_LOAD_PARALLELISM_HIGH, config.getParallelism());
        assertFalse(config.isMonitor()); // boolean default is false

        // Test toString method (generated by Lombok)
        String toString = config.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("BulkLoadConfig"));
        assertTrue(toString.contains(TestDataProvider.BUCKET));
        assertTrue(toString.contains(TestDataProvider.NEPTUNE_ENDPOINT));
    }
}
