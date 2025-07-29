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

import lombok.Data;
import lombok.NoArgsConstructor;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Configuration class for Neptune bulk load settings from YAML file.
 * <p>
 * Expected YAML format:
 * bucket-name: "my-s3-bucket"
 * s3-prefix: "neptune"
 * neptune-endpoint: "my-neptune-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com"
 * iam-role-arn: "arn:aws:iam::123456789012:role/NeptuneLoadFromS3"
 * parallelism: "OVERSUBSCRIBE"
 * monitor: true
 */
@Data
@NoArgsConstructor
public class BulkLoadConfig {

    private String bucketName;
    private String s3Prefix;
    private String neptuneEndpoint;
    private String iamRoleArn;
    private String parallelism;
    private boolean monitor;
    private static final String DEFAULT_S3_PREFIX = "";
    private static final String DEFAULT_PARALLELISM = "OVERSUBSCRIBE";
    private static final boolean DEFAULT_MONITOR = false;

    public static BulkLoadConfig fromFile(File configFile) throws IOException {
        BulkLoadConfig config = new BulkLoadConfig();

        if (configFile != null && configFile.exists()) {
            Yaml yaml = new Yaml();
            try (FileInputStream inputStream = new FileInputStream(configFile)) {
                Map<String, Object> yamlData = yaml.load(inputStream);
                config.loadFromYaml(yamlData);
            }
        }

        return config;
    }

    private void loadFromYaml(Map<String, Object> yamlData) {
        if (yamlData != null) {
            this.bucketName = getStringValue(yamlData, "bucket-name");
            this.s3Prefix = getStringValue(yamlData, "s3-prefix", DEFAULT_S3_PREFIX);
            this.neptuneEndpoint = getStringValue(yamlData, "neptune-endpoint");
            this.iamRoleArn = getStringValue(yamlData, "iam-role-arn");
            this.parallelism = getStringValue(yamlData, "parallelism", DEFAULT_PARALLELISM);
            this.monitor = getBooleanValue(yamlData, "monitor", DEFAULT_MONITOR);
        }
    }

    private String getStringValue(Map<String, Object> yamlData, String key) {
        return getStringValue(yamlData, key, null);
    }

    private String getStringValue(Map<String, Object> yamlData, String key, String defaultValue) {
        Object value = yamlData.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private boolean getBooleanValue(Map<String, Object> yamlData, String key, boolean defaultValue) {
        Object value = yamlData.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            throw new IllegalArgumentException("Expected boolean value for " + key);
        }
    }

    public BulkLoadConfig withBucketName(String bucketName) {
        if (bucketName != null && !bucketName.trim().isEmpty()) {
            this.bucketName = bucketName;
        }
        return this;
    }

    public BulkLoadConfig withNeptuneEndpoint(String neptuneEndpoint) {
        if (neptuneEndpoint != null && !neptuneEndpoint.trim().isEmpty()) {
            this.neptuneEndpoint = neptuneEndpoint;
        }
        return this;
    }

    public BulkLoadConfig withIamRoleArn(String iamRoleArn) {
        if (iamRoleArn != null && !iamRoleArn.trim().isEmpty()) {
            this.iamRoleArn = iamRoleArn;
        }
        return this;
    }

    /**
     * Validates the bulk load configuration
     * @param config The configuration to validate
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public static void validateBulkLoadConfigFile(BulkLoadConfig config) throws IllegalArgumentException {
        // Collect missing required parameters
        StringBuilder errorMsg = new StringBuilder();

        // Check required fields
        if (isNullOrEmpty(config.getNeptuneEndpoint())) {
            errorMsg.append("  - Neptune endpoint (--neptune-endpoint)\n");
        }

        if (isNullOrEmpty(config.getBucketName())) {
            errorMsg.append("  - S3 bucket name (--bucket-name)\n");
        }

        if (isNullOrEmpty(config.getIamRoleArn())) {
            errorMsg.append("  - IAM role ARN (--iam-role-arn)\n");
        }

        // If any required fields are missing, throw exception with all missing fields
        if (errorMsg.length() > 0) {
            throw new IllegalArgumentException(
                "Error: Missing required bulk load parameters. " +
                "Please ensure the following are provided either via CLI or config file:\n" + errorMsg);
        }

        // Validate parallelism if present
        String parallelism = config.getParallelism();
        if (!isNullOrEmpty(parallelism)) {
            Set<String> validParallelismOptions = Set.of("LOW", "MEDIUM", "HIGH", "OVERSUBSCRIBE");
            if (!validParallelismOptions.contains(parallelism.toUpperCase())) {
                throw new IllegalArgumentException("Parallelism must be one of: LOW, MEDIUM, HIGH, OVERSUBSCRIBE");
            }
        }
    }

    /**
     * Helper method to check if a string is null or empty
     * @param str The string to check
     * @return true if the string is null or empty, false otherwise
     */
    private static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }
}
