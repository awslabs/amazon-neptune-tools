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

package com.amazonaws.services.neptune.util;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Utility class for uploading local CSV files to Amazon S3 and loading them into Neptune
 * Designed for Neptune data loading workflows with bulk loading capability
 */
public class NeptuneBulkLoader implements AutoCloseable {
    private static final Set<String> BULK_LOAD_STATUS_CODES_COMPLETED;
    private static final Set<String> BULK_LOAD_STATUS_CODES_FAILURES;
    private static final int MAX_RETRIES = 3;
    private static final int INITIAL_BACKOFF_MS = 1000;
    private static final int CONNECTION_TIMEOUT_SECONDS = 30;
    private static final int REQUEST_TIMEOUT_SECONDS = 120;
    private static final int MONITOR_SLEEP_TIME_MS = 1000;
    private static final int MONITOR_MAX_ATTEMPTS = 300;

    static {
        Set<String> completed = new HashSet<>();
        completed.add("LOAD_COMPLETED");
        completed.add("LOAD_COMMITTED_W_WRITE_CONFLICTS");
        BULK_LOAD_STATUS_CODES_COMPLETED = Collections.unmodifiableSet(completed);

        Set<String> failures = new HashSet<>();
        failures.add("LOAD_CANCELLED_BY_USER");
        failures.add("LOAD_CANCELLED_DUE_TO_ERRORS");
        failures.add("LOAD_UNEXPECTED_ERROR");
        failures.add("LOAD_FAILED");
        failures.add("LOAD_S3_READ_ERROR");
        failures.add("LOAD_S3_ACCESS_DENIED_ERROR");
        failures.add("LOAD_DATA_DEADLOCK");
        failures.add("LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED");
        failures.add("LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED");
        failures.add("LOAD_FAILED_INVALID_REQUEST");
        failures.add("LOAD_CANCELLED");
        BULK_LOAD_STATUS_CODES_FAILURES = Collections.unmodifiableSet(failures);
    }

    private static final String NEPTUNE_PORT = "8182"; // Default Neptune port for HTTP API
    private final S3AsyncClient s3AsyncClient;
    private final String bucketName;
    private final String s3Prefix;
    private final Region region;
    private final String neptuneEndpoint;
    private final String iamRoleArn;
    private final String parallelism;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public NeptuneBulkLoader(
            String bucketName, String s3Prefix, String neptuneEndpoint, String iamRoleArn, String parallelism) {

        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty");
        }
        this.bucketName = bucketName.replaceAll("/+$", "");

        if (s3Prefix == null || s3Prefix.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 prefix cannot be empty");
        }
        this.s3Prefix = s3Prefix.replaceAll("/+$", "");

        if (neptuneEndpoint == null || neptuneEndpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("Neptune endpoint cannot be null or empty");
        }
        this.neptuneEndpoint = neptuneEndpoint;

        // Example endpoint: my-neptune-cluster.cluster[-custom]-abc123.<region>.neptune.amazonaws.com
        String[] endpointParts = neptuneEndpoint.split("\\.");
        if (endpointParts.length < 4) {
            throw new IllegalArgumentException("Invalid Neptune endpoint format: " + neptuneEndpoint);
        }
        this.region = Region.of(endpointParts[2]);

        if (iamRoleArn == null || iamRoleArn.trim().isEmpty()) {
            throw new IllegalArgumentException("IAM role ARN cannot be null or empty");
        }
        this.iamRoleArn = iamRoleArn;

        if (parallelism == null || parallelism.trim().isEmpty()) {
            throw new IllegalArgumentException("Parallelism cannot be null or empty");
        }
        this.parallelism = parallelism;

        this.objectMapper = new ObjectMapper();

        this.s3AsyncClient = S3AsyncClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS))
                .build();

        // Validate for not empty parameters
        System.err.println("S3 Bucket: " + this.bucketName);
        System.err.println("S3 Prefix: " + this.s3Prefix);
        System.err.println("AWS Region: " + this.region);
        System.err.println("IAM Role ARN: " + this.iamRoleArn);
        System.err.println("Neptune Endpoint: " + this.neptuneEndpoint);
        System.err.println("Bulk Load Parallelism: " + this.parallelism);
    }

    // Constructor for testing
    public NeptuneBulkLoader(String bucketName, String s3Prefix,
            String neptuneEndpoint, String iamRoleArn, String parallelism,
            HttpClient httpClient, S3AsyncClient s3AsyncClient) {
        this.bucketName = bucketName;
        this.s3Prefix = s3Prefix;
        this.neptuneEndpoint = neptuneEndpoint;
        // Example endpoint: my-neptune-cluster.cluster[-custom]-abc123.<region>.neptune.amazonaws.com
        this.region = Region.of(neptuneEndpoint.split("\\.")[2]);
        this.iamRoleArn = iamRoleArn;
        this.parallelism = parallelism;
        this.objectMapper = new ObjectMapper();
        this.s3AsyncClient = s3AsyncClient;
        this.httpClient = httpClient;
    }

    /**
     * Upload Neptune vertices and edges CSV files asynchronously
     */
    public String uploadCsvFilesToS3(String filePath) throws Exception {
        System.err.println("Uploading Gremlin load data to S3...");

        // Grab the timestamp of ConvertCsv to use as S3 directory prefix
        String convertCsvTimeStamp = filePath.substring(filePath.lastIndexOf('/') + 1);

        // Check if the S3 prefix is provided, and construct the full S3 prefix using convertCsvTimeStamp
        String s3PrefixWithTimeStamp = Optional.ofNullable(s3Prefix)
            .filter(prefix  -> !prefix.isEmpty())
            .map(prefix  -> prefix + "/")
            .orElse("") + convertCsvTimeStamp;

        // Upload all files from the directory
        CompletableFuture<Boolean> uploadFuture = uploadFileAsync(filePath, s3PrefixWithTimeStamp);

        // Wait for upload to complete
        uploadFuture.get();

        // Check result
        boolean uploadSuccess = uploadFuture.get();
        if (!uploadSuccess) {
            System.err.println("CSV file uploads failed from directory: " + filePath);
            throw new RuntimeException("One or more CSV uploads failed.");
        }
        String uploadS3Uri = "s3://" + bucketName + "/" + s3PrefixWithTimeStamp+ "/";
        System.err.println("Files uploaded successfully to S3. Files available at: " + uploadS3Uri);
        return uploadS3Uri;
    }

    /**
     * Upload all files from a directory to S3 asynchronously
     */
    protected CompletableFuture<Boolean> uploadFileAsync(String directoryPath, String s3Prefix) throws Exception {
        // Create a File object to check existence
        File directory = new File(directoryPath);

        if (!directory.exists() || !directory.isDirectory()) {
            throw new IllegalStateException("Directory does not exist: " + directoryPath);
        }

        System.err.println("Starting async upload of files from " +
            directoryPath + " to s3://" + bucketName + "/" + s3Prefix);

        // Get all CSV files in the directory
        File[] csvFiles = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            System.err.println("No files with correct extension were found in " + directoryPath);
            return CompletableFuture.completedFuture(false);
        }

        // Create a list to hold all upload futures
        List<CompletableFuture<Boolean>> uploadFutures = new ArrayList<>();

        // Start upload for each CSV file
        for (File csvFile : csvFiles) {
            String csvFilePath = s3Prefix + "/" + csvFile.getName();
            CompletableFuture<Boolean> uploadFuture = uploadSingleFileAsync(csvFile.getAbsolutePath(), csvFilePath);
            uploadFutures.add(uploadFuture);
        }

        // Combine all futures and return true only if all uploads succeed
        return CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                boolean allSuccessful = uploadFutures.stream()
                    .allMatch(future -> {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            System.err.println("Error getting upload result: " + e.getMessage());
                            return false;
                        }
                    });

                if (allSuccessful) {
                    System.err.println("Successfully uploaded " + csvFiles.length + " files from " + directoryPath);
                } else {
                    System.err.println("Failed uploading file(s) from " + directoryPath);
                }

                return allSuccessful;
            });
    }

    /**
     * Upload a single CSV file to S3 asynchronously (helper method)
     */
    protected CompletableFuture<Boolean> uploadSingleFileAsync(String localFilePath, String s3Prefix) throws Exception {
        // Create a File object to check existence
        File file = new File(localFilePath);

        if (!file.exists() || !file.isFile()) {
            throw new IllegalStateException("File does not exist: " + localFilePath);
        }

        String s3SourceUri = "s3://" + bucketName + "/" + s3Prefix;
        System.err.println("Starting async upload of " + localFilePath + " to " + s3SourceUri);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Prefix)
                .contentType("text/csv")
                .build();

        // Create async request body from file
        AsyncRequestBody requestBody = AsyncRequestBody.fromFile(file);

        // Start async upload
        CompletableFuture<PutObjectResponse> uploadFuture = s3AsyncClient.putObject(putObjectRequest, requestBody);

        // Return a future that resolves to boolean success
        return uploadFuture.handle((response, throwable) -> {
            if (throwable != null) {
                if (throwable instanceof S3Exception) {
                    System.err.println("S3 error uploading file " + localFilePath + ": " + throwable);
                } else {
                    System.err.println("Unexpected error uploading file " + localFilePath + ": " + throwable);
                }
                return false;
            } else {
                System.err.println("Successfully uploaded " + file.getName() + " - ETag: " + response.eTag());
                return true;
            }
        });
    }

    /**
     * Start Neptune bulk load job with automatic fallback
     */
    public String startNeptuneBulkLoad(String s3SourceUri) throws Exception {
        System.err.println("Starting Neptune bulk load...");
        if (!testNeptuneConnectivity()) {
            throw new RuntimeException("Cannot connect to Neptune endpoint: " + neptuneEndpoint);
        }

        HttpRequest request = buildBulkLoadRequest(s3SourceUri);

        // Retry configuration
        HttpResponse<String> response = null;
        String loadId = null;

        // Retry loop with exponential backoff
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    throw new RuntimeException("Failed to start Neptune bulk load. Status: " +
                        response.statusCode() + " Response: " + response.body());
                }

                JsonNode responseJson = objectMapper.readTree(response.body());

                loadId = responseJson.get("payload").get("loadId").asText();
                if (loadId == null) {
                    throw new RuntimeException("Failed to start Neptune bulk load with payload: " +
                        responseJson.get("payload"));
                }
                System.err.println("Neptune bulk load started successfully! Load ID: " + loadId);
                return loadId;
            } catch (Exception e) {
                if (attempt == MAX_RETRIES) {
                    // Use response null check to avoid potential NPE
                    String errorDetails = (response != null)
                        ? "Status: " + response.statusCode() + " Response: " + response.body()
                        : "No response received";
                    String errorMessage = "Failed to start Neptune bulk load after " +
                        (MAX_RETRIES + 1) + " attempts. " + errorDetails;
                    System.err.println(errorMessage);
                    throw new RuntimeException(errorMessage, e);
                }
                System.err.println("Attempt " + (attempt + 1) + " failed: " + e.getMessage());
                try {
                    Thread.sleep(INITIAL_BACKOFF_MS * (1L << attempt)); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // Restore interrupt status
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }
        return loadId;
    }

    private HttpRequest buildBulkLoadRequest(String s3SourceUri) {
        String loaderEndpoint = "https://" + neptuneEndpoint + ":" + NEPTUNE_PORT + "/loader";
        String requestBody = createRequestBody(s3SourceUri);

        return HttpRequest.newBuilder()
                .uri(URI.create(loaderEndpoint))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS))
                .build();
    }

    private String createRequestBody(String s3SourceUri) {
        return String.format(
            "{\n" +
            "  \"source\": \"%s\",\n" +
            "  \"format\": \"csv\",\n" +
            "  \"iamRoleArn\": \"%s\",\n" +
            "  \"region\": \"%s\",\n" +
            "  \"failOnError\": \"FALSE\",\n" +
            "  \"parallelism\": \"%s\",\n" +
            "  \"updateSingleCardinalityProperties\": \"FALSE\",\n" +
            "  \"queueRequest\": \"TRUE\"\n" +
            "}",
            s3SourceUri, iamRoleArn, region, parallelism
        );
    }

    /**
     * Test connectivity to Neptune endpoint
     */
    protected boolean testNeptuneConnectivity() {
        try {
            System.err.println("Testing connectivity to Neptune endpoint...");
            String testEndpoint = "https://" + neptuneEndpoint + ":" + NEPTUNE_PORT + "/status";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(testEndpoint))
                    .header("Content-Type", "application/json")
                    .GET()
                    .timeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                System.err.println("Failed to connect to Neptune status endpoint. Status: " + response.statusCode());
                return false;
            }

            JsonNode responseBody = objectMapper.readTree(response.body());
            if (!responseBody.has("status") ||
                    !responseBody.get("status").asText().equals("healthy")) {
                throw new RuntimeException("Status not found or instance is not healthy: " + responseBody);
            }

            System.err.println("Successful connected to Neptune. Status: " +
                response.statusCode() + " " + responseBody.get("status").asText());
            return true;
        } catch (Exception e) {
            System.err.println("Neptune connectivity test failed: " + e.getLocalizedMessage());
            return false;
        }
    }

    /**
     * Monitor Neptune bulk load progress
     */
    public void monitorLoadProgress(String loadId) throws Exception {
        System.err.println("Monitoring load progress for job: " + loadId);
        int attempt = 0;

        while (attempt < MONITOR_MAX_ATTEMPTS) {
            String statusResponse = checkNeptuneBulkLoadStatus(loadId);

            if (statusResponse != null) {
                JsonNode responseJson = objectMapper.readTree(statusResponse);
                String status = "UNKNOWN";

                if (responseJson.has("payload") &&
                        responseJson.get("payload").has("overallStatus")) {
                    status = responseJson.get("payload")
                        .get("overallStatus").get("status").asText();
                } else if (responseJson.has("status")) {
                    status = responseJson.get("status").asText();
                }

                if (BULK_LOAD_STATUS_CODES_COMPLETED.contains(status)) {
                    System.err.println("Neptune bulk load completed with status: " + status);
                    break;
                } else if (BULK_LOAD_STATUS_CODES_FAILURES.contains(status)) {
                    System.err.println("Neptune bulk load failed with status: " + status);
                    System.err.println("Full response: " + statusResponse);
                    break;
                } else {
                    System.err.println("Neptune bulk load status: " + status);
                }
            }

            Thread.sleep(MONITOR_SLEEP_TIME_MS);
            attempt++;
        }

        if (attempt >= MONITOR_MAX_ATTEMPTS) {
            System.err.println("Monitoring timeouted at " +
                MONITOR_SLEEP_TIME_MS * MONITOR_MAX_ATTEMPTS + "ms. Check load status manually.");
        }
    }

    /**
     * Check the status of a Neptune bulk load job via HTTP
     */
    protected String checkNeptuneBulkLoadStatus(String loadId) throws Exception {
        String statusEndpoint = "https://" + neptuneEndpoint + ":" + NEPTUNE_PORT + "/loader/" + loadId;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(statusEndpoint))
                .header("Content-Type", "application/json")
                .GET()
                .timeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS))
                .build();

        HttpResponse<String> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            return response.body();
        } else {
            throw new RuntimeException("Request failed with code " + response.statusCode() + ": " + response.body());
        }
    }

    /**
     * Close the S3 async client and release resources (AutoCloseable implementation)
     */
    @Override
    public void close() {
        if (s3AsyncClient != null) {
            s3AsyncClient.close();
        }
    }
}
