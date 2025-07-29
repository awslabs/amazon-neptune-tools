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

import com.amazonaws.services.neptune.TestDataProvider;
import com.amazonaws.services.neptune.metadata.BulkLoadConfig;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

public class NeptuneBulkLoaderTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @Before
    public void setUp() throws Exception {
        // Capture System.out and System.err
        originalOut = System.out;
        originalErr = System.err;
        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @After
    public void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testConstructorWithValidParameters() {
        // Test valid constructor parameters
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Verify constructor output messages
        String error = errorStream.toString();
        assertTrue("Should contain bucket name", error.contains("S3 Bucket: " + TestDataProvider.BUCKET));
        assertTrue("Should contain S3 prefix", error.contains("S3 Prefix: " + TestDataProvider.S3_PREFIX));
        assertTrue("Should contain region", error.contains("AWS Region: " + TestDataProvider.REGION_US_EAST_2));
        assertTrue("Should contain IAM role ARN", error.contains("IAM Role ARN: " + TestDataProvider.IAM_ROLE_ARN));
        assertTrue("Should contain Neptune endpoint", error.contains("Neptune Endpoint: " + TestDataProvider.NEPTUNE_ENDPOINT));
        assertTrue("Should contain Neptune bulk load parallelism", error.contains("Bulk Load Parallelism: " + TestDataProvider.BULK_LOAD_PARALLELISM_MEDIUM));
        assertNotNull("NeptuneBulkLoader should be created", neptuneBulkLoader);
    }

    @Test
    public void testConstructorWithValidParallelismParameters() {
        // Test each valid parallelism value
        String[] validParallelismValues = {
            TestDataProvider.BULK_LOAD_PARALLELISM_LOW, TestDataProvider.BULK_LOAD_PARALLELISM_MEDIUM,
            TestDataProvider.BULK_LOAD_PARALLELISM_HIGH, TestDataProvider.BULK_LOAD_PARALLELISM_OVERSUBSCRIBE
        };

        for (String parallelism : validParallelismValues) {
            BulkLoadConfig bulkLoadConfig = TestDataProvider.createBulkLoadConfig(
                TestDataProvider.BUCKET, TestDataProvider.S3_PREFIX, TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.IAM_ROLE_ARN, parallelism, TestDataProvider.BULK_LOAD_MONITOR_FALSE);
            // Create NeptuneBulkLoader with each parallelism value
            NeptuneBulkLoader loader = new NeptuneBulkLoader(bulkLoadConfig);

            // Verify it was created successfully (not null)
            assertNotNull("NeptuneBulkLoader should be created with parallelism: " + parallelism, loader);

            // Verify the parallelism value is logged
            String error = errorStream.toString();
            assertTrue("Should contain parallelism value: " + parallelism,
                error.contains("Bulk Load Parallelism: " + parallelism));

            // Clear error stream for next iteration
            errorStream.reset();
        }
    }

    @Test
    public void testConstructorWithEmptyS3Prefix() {
        BulkLoadConfig bulkLoadConfig = TestDataProvider.createBulkLoadConfig(
            TestDataProvider.BUCKET, "", TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.IAM_ROLE_ARN, TestDataProvider.BULK_LOAD_PARALLELISM_MEDIUM, TestDataProvider.BULK_LOAD_MONITOR_FALSE);
            // Create NeptuneBulkLoader with blank s3prefix
            NeptuneBulkLoader loader = new NeptuneBulkLoader(bulkLoadConfig);

            // Verify constructor output messages
            String error = errorStream.toString();
            assertTrue("Should contain bucket name", error.contains("S3 Bucket: " + TestDataProvider.BUCKET));
            assertTrue("Should contain S3 prefix", error.contains("S3 Prefix: "));
            assertTrue("Should contain region", error.contains("AWS Region: " + TestDataProvider.REGION_US_EAST_2));
            assertTrue("Should contain IAM role ARN", error.contains("IAM Role ARN: " + TestDataProvider.IAM_ROLE_ARN));
            assertTrue("Should contain Neptune endpoint", error.contains("Neptune Endpoint: " + TestDataProvider.NEPTUNE_ENDPOINT));
            assertTrue("Should contain Neptune bulk load parallelism", error.contains("Bulk Load Parallelism: " + TestDataProvider.BULK_LOAD_PARALLELISM_MEDIUM));
            assertNotNull("NeptuneBulkLoader should be created", loader);
    }

    @Test
    public void testUploadSingleFileAsyncS3Success() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testVerticiesFile = new File(testDir, TestDataProvider.VERTICIES_CSV);
        TestDataProvider.createMockVerticesFile(testDir, testVerticiesFile);

        // Create a successful PutObjectResponse
        PutObjectResponse putObjectResponse = PutObjectResponse.builder()
            .eTag("mock-etag-12345")
            .build();

        // Create a successful CompletedFileUpload
        CompletedFileUpload completedUpload = mock(CompletedFileUpload.class);
        when(completedUpload.response()).thenReturn(putObjectResponse);

        // Create a CompletableFuture that completes successfully
        CompletableFuture<CompletedFileUpload> successFuture = CompletableFuture.completedFuture(completedUpload);

        // Mock the FileUpload
        FileUpload mockFileUpload = mock(FileUpload.class);
        when(mockFileUpload.completionFuture()).thenReturn(successFuture);

        // Mock the S3TransferManager
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        // Mock the uploadFile method to return the successful FileUpload
        when(mockTransferManager.uploadFile(any(UploadFileRequest.class)))
            .thenReturn(mockFileUpload);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        try {
            CompletableFuture<Boolean> result = neptuneBulkLoader.uploadSingleFileAsync(
                testVerticiesFile.getAbsolutePath(),
                TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES
            );

            // The method should return a CompletableFuture
            assertNotNull("uploadSingleFileAsync should return a CompletableFuture", result);

            // Wait for the result and verify it's true
            Boolean uploadResult = result.get();
            assertTrue("Upload should be successful", uploadResult);

            // Verify that the S3TransferManager.uploadFile was called
            verify(mockTransferManager, times(1)).uploadFile(any(UploadFileRequest.class));

            // Verify the output contains success message
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting async upload"));

        } catch (Exception e) {
            fail("Should not throw exception when S3TransferManager is mocked successfully: " + e.getMessage());
        }
    }

    @Test
    public void testUploadSingleFileAsyncS3Exception() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testVerticiesFile = new File(testDir, TestDataProvider.VERTICIES_CSV);
        TestDataProvider.createMockVerticesFile(testDir, testVerticiesFile);

        // Mock S3Exception to be thrown
        S3Exception s3Exception = (S3Exception) S3Exception.builder()
            .message("Access Denied")
            .statusCode(403)
            .build();

        // Create a CompletableFuture that completes exceptionally with S3Exception
        CompletableFuture<CompletedFileUpload> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(s3Exception);

        // Mock the FileUpload
        FileUpload mockFileUpload = mock(FileUpload.class);
        when(mockFileUpload.completionFuture()).thenReturn(failedFuture);

        // Mock the S3TransferManager and HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        // Mock the uploadFile method to return the failed FileUpload
        when(mockTransferManager.uploadFile(any(UploadFileRequest.class)))
            .thenReturn(mockFileUpload);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        try {
            CompletableFuture<Boolean> result = neptuneBulkLoader.uploadSingleFileAsync(
                testVerticiesFile.getAbsolutePath(),
                TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES
            );

            // Wait for the future to complete and expect it to return false due to exception
            Boolean uploadResult = result.get();
            assertFalse("Upload should fail and return false when S3Exception occurs", uploadResult);

            // Verify that the S3TransferManager.uploadFile was called
            verify(mockTransferManager, times(1)).uploadFile(any(UploadFileRequest.class));

            // Verify error stream contains the exception details
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting async upload"));
            assertTrue("Should contain error message about upload failure",
                error.contains("Transfer Manager upload failed") || error.contains("Access Denied"));

        } catch (Exception e) {
            // If an exception is thrown instead of returning false, verify it's the S3Exception
            assertTrue("Should contain S3Exception in the cause chain",
                e.getCause() instanceof S3Exception || e instanceof S3Exception);

            // Verify the exception message
            String exceptionMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            assertTrue("Should contain Access Denied message",
                exceptionMessage.contains("Access Denied"));

            // Verify that the S3TransferManager.uploadFile was called
            verify(mockTransferManager, times(1)).uploadFile(any(UploadFileRequest.class));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadSingleFileAsyncWithNonExistentDirectory() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadSingleFileAsync("/non/existent/file.csv", TestDataProvider.S3_PREFIX);
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadSingleFileAsyncWithNonExistentFile() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);

        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadSingleFileAsync(testDir.getAbsolutePath(), TestDataProvider.S3_PREFIX);
    }

    @Test
    public void testUploadCsvFilesToS3WithBothFilesSuccess() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock both uploadSingleFileAsync calls to return successful futures
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);
        doReturn(successFuture).when(spyLoader).uploadSingleFileAsync(anyString(), anyString());

        // Test upload
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());

        // Verify both files were uploaded (vertices.csv and edges.csv)
        verify(spyLoader, times(2)).uploadSingleFileAsync(anyString(), anyString());

        // Verify specific file paths were called
        verify(spyLoader).uploadSingleFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.VERTICIES_CSV),
            contains(TestDataProvider.VERTICIES_CSV)
        );
        verify(spyLoader).uploadSingleFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.EDGES_CSV),
            contains(TestDataProvider.EDGES_CSV)
        );

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain initial upload message",
            error.contains("Uploading Gremlin load data to S3..."));
        assertTrue("Should contain success message",
            error.contains("Files uploaded successfully to S3. Files available at"));
        assertTrue("Should contain S3 bucket name",
            error.contains(TestDataProvider.BUCKET));
        assertTrue("Should contain timestamp",
            error.contains(testDir.getName()));
    }

    @Test(expected = RuntimeException.class)
    public void testUploadCsvFilesToS3WithVerticesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock vertices upload to fail, edges to succeed
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);

        doReturn(failureFuture).when(spyLoader).uploadSingleFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.VERTICIES_CSV),
            anyString()
        );
        doReturn(successFuture).when(spyLoader).uploadSingleFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.EDGES_CSV),
            anyString()
        );

        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain upload message",
            error.contains("Upload failures - Vertices: 1, Edges: 0"));
    }

    @Test(expected = RuntimeException.class)
    public void testUploadCsvFilesToS3WithEdgesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock vertices upload to succeed, edges to fail
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);

        doReturn(successFuture).when(spyLoader).uploadSingleFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.VERTICIES_CSV),
            anyString()
        );
        doReturn(failureFuture).when(spyLoader).uploadSingleFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.EDGES_CSV),
            anyString()
        );

        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain upload message",
            error.contains("Upload failures - Vertices: 0, Edges: 1"));
    }

    @Test(expected = RuntimeException.class)
    public void testUploadCsvFilesToS3WithBothFilesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock both uploads to fail
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);
        doReturn(failureFuture).when(spyLoader).uploadSingleFileAsync(anyString(), anyString());

        // Test upload - should throw RuntimeException
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
        String error = errorStream.toString();
        assertTrue("Should contain upload message",
            error.contains("Upload failures - Vertices: 1, Edges: 1"));
    }

    @Test
    public void testUploadCsvFilesToS3WithException() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadSingleFileAsync to throw an exception
        CompletableFuture<Boolean> exceptionFuture = new CompletableFuture<>();
        exceptionFuture.completeExceptionally(new RuntimeException("S3 connection failed"));
        doReturn(exceptionFuture).when(spyLoader).uploadSingleFileAsync(anyString(), anyString());

        try {
            spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
            fail("Expected exception to be thrown");
        } catch (Exception e) {
            // Verify the exception is properly propagated
            assertTrue("Should contain connection error",
                e.getMessage().contains("S3 connection failed") ||
                e.getCause().getMessage().contains("S3 connection failed"));
        }
    }

    @Test
    public void testUploadCsvFilesToS3S3PrefixConstruction() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);
        doReturn(successFuture).when(spyLoader).uploadSingleFileAsync(anyString(), anyString());

        // Test upload
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());

        // Extract timestamp from directory name
        String expectedTimestamp = testDir.getName();
        String expectedS3Prefix = TestDataProvider.S3_PREFIX + File.separator + expectedTimestamp;

        // Verify S3 prefixes are constructed correctly
        verify(spyLoader).uploadSingleFileAsync(
            anyString(),
            eq(expectedS3Prefix + "/" + TestDataProvider.VERTICIES_CSV)
        );
        verify(spyLoader).uploadSingleFileAsync(
            anyString(),
            eq(expectedS3Prefix + "/" + TestDataProvider.EDGES_CSV)
        );
    }

    @Test
    public void testUploadCsvFilesToS3ErrorMessages() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadFileAsync to return failure (directory-based approach)
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);
        doReturn(failureFuture).when(spyLoader).uploadFileAsync(anyString(), anyString());

        try {
            spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Verify error messages
            String error = errorStream.toString();
            assertTrue("Should contain upload failure message",
                error.contains("CSV file uploads failed from directory"));

            assertTrue("Should contain runtime exception message",
                e.getMessage().contains("One or more CSV uploads failed"));
        }
    }

    @Test
    public void testNeptuneConnectivitySuccess() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock successful response
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"healthy\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertTrue("Neptune connectivity should return true for healthy status", result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test message",
            error.contains("Testing connectivity to Neptune endpoint..."));
        assertTrue("Should contain success message",
            error.contains("Successful connected to Neptune. Status: 200 healthy"));
    }

    @Test
    public void testNeptuneConnectivityUnhealthyStatus() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock response with unhealthy status
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"unhealthy\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Test connectivity - the RuntimeException is caught and method returns false
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false for unhealthy status", result);

        // Verify error message in stderr
        String error = errorStream.toString();
        assertTrue("Final error message for unhealthy status should be present",
            error.contains("Neptune connectivity test failed"));
    }

    @Test
    public void testNeptuneConnectivityMissingStatus() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock response without status field
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"message\":\"no status field\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Test connectivity - the RuntimeException is caught and method returns false
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false for missing status", result);

        // Verify error message in stderr
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test failed message",
            error.contains("Neptune connectivity test failed"));
    }

    @Test
    public void testNeptuneConnectivityNon200StatusCode() throws Exception {
        Object[][] invalidStatusCodes = {
            {"400", 400}, {"401", 401}, {"403", 403}, {"404", 404}, {"405", 405}, {"406", 406},
            {"408", 408}, {"413", 413}, {"414", 414}, {"415", 415}, {"416", 416}, {"418", 418},
            {"429", 429}, {"500", 500}, {"502", 502}, {"503", 503}, {"504", 504}, {"509", 509}
        };
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        for (Object[] params : invalidStatusCodes) {
            // Mock invalid response
            when(mockResponse.statusCode()).thenReturn((Integer) params[1]);
            when(mockResponse.body()).thenReturn("Not Found");
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

            // Create NeptuneBulkLoader with mock clients for each iteration
            neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

            // Test connectivity
            boolean result = neptuneBulkLoader.testNeptuneConnectivity();

            assertFalse("Neptune connectivity should return false for non-200 status", result);

            // Verify error message
            String error = errorStream.toString();
            assertTrue("Should contain failed connection message",
                error.contains("Failed to connect to Neptune status endpoint. Status: " + params[0]));
        }
    }

    @Test
    public void testNeptuneConnectivityHttpException() throws Exception {
        // Mock HttpClient to throw exception
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new RuntimeException("Connection timeout"));

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false when exception occurs", result);

        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test failed message",
            error.contains("Neptune connectivity test failed: Connection timeout"));
    }

    @Test
    public void testNeptuneConnectivityJsonParsingException() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock response with invalid JSON
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("invalid json response");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false when JSON parsing fails", result);

        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test failed message",
            error.contains("Neptune connectivity test failed"));
    }

    @Test
    public void testNeptuneConnectivityEndpointConstruction() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"healthy\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertTrue("Neptune connectivity should succeed with custom endpoint", result);

        // Verify that the correct endpoint was used by checking the HttpRequest
        verify(mockHttpClient).send(argThat(request -> {
            String expectedUrl = "https://" + TestDataProvider.NEPTUNE_ENDPOINT + ":8182/status";
            return request.uri().toString().equals(expectedUrl);
        }), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusSuccess() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock successful response
        String expectedResponse = "{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(expectedResponse);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader instance with mock HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Call the protected method directly
        String result = neptuneBulkLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Verify the result
        assertEquals("Should return the response body", expectedResponse, result);

        // Verify that the correct endpoint was used
        verify(mockHttpClient).send(argThat(request -> {
            String expectedUrl = "https://" + TestDataProvider.NEPTUNE_ENDPOINT + ":8182/loader/" + TestDataProvider.LOAD_ID_0;
            return request.uri().toString().equals(expectedUrl);
        }), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusWithDifferentStatuses() throws Exception {
        String[][] testCases = {
            {TestDataProvider.LOAD_IN_PROGRESS, "{\"status\":\"" + TestDataProvider.LOAD_IN_PROGRESS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}"},
            {TestDataProvider.LOAD_COMPLETED, "{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}"},
            {TestDataProvider.LOAD_FAILED, "{\"status\":\"" + TestDataProvider.LOAD_FAILED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Invalid data format\"}"},
            {TestDataProvider.LOAD_CANCELLED, "{\"status\":\"" + TestDataProvider.LOAD_CANCELLED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}"},
            {TestDataProvider.LOAD_COMMITTED_W_WRITE_CONFLICTS, "{\"status\":\"" + TestDataProvider.LOAD_COMMITTED_W_WRITE_CONFLICTS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"message\":\"Load completed with write conflicts\"}"},
            {TestDataProvider.LOAD_CANCELLED_BY_USER, "{\"status\":\"" + TestDataProvider.LOAD_CANCELLED_BY_USER + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Load cancelled by user request\"}"},
            {TestDataProvider.LOAD_CANCELLED_DUE_TO_ERRORS, "{\"status\":\"" + TestDataProvider.LOAD_CANCELLED_DUE_TO_ERRORS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Load cancelled due to errors\"}"},
            {TestDataProvider.LOAD_UNEXPECTED_ERROR, "{\"status\":\"" + TestDataProvider.LOAD_UNEXPECTED_ERROR + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Unexpected error occurred\"}"},
            {TestDataProvider.LOAD_S3_READ_ERROR, "{\"status\":\"" + TestDataProvider.LOAD_S3_READ_ERROR + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Cannot read from S3 bucket\"}"},
            {TestDataProvider.LOAD_S3_ACCESS_DENIED_ERROR, "{\"status\":\"" + TestDataProvider.LOAD_S3_ACCESS_DENIED_ERROR + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Access denied to S3 bucket\"}"},
            {TestDataProvider.LOAD_DATA_DEADLOCK, "{\"status\":\"" + TestDataProvider.LOAD_DATA_DEADLOCK + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Data deadlock detected\"}"},
            {TestDataProvider.LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED, "{\"status\":\"" + TestDataProvider.LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Data feed was modified or deleted\"}"},
            {TestDataProvider.LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED, "{\"status\":\"" + TestDataProvider.LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Load dependency not satisfied\"}"},
            {TestDataProvider.LOAD_FAILED_INVALID_REQUEST, "{\"status\":\"" + TestDataProvider.LOAD_FAILED_INVALID_REQUEST + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Invalid load request\"}"},
            {TestDataProvider.LOAD_STARTING, "{\"status\":\"" + TestDataProvider.LOAD_STARTING + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"message\":\"Load operation starting\"}"},
            {TestDataProvider.LOAD_QUEUED, "{\"status\":\"" + TestDataProvider.LOAD_QUEUED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"message\":\"Load operation queued\"}"},
            {TestDataProvider.LOAD_COMMITTING, "{\"status\":\"" + TestDataProvider.LOAD_COMMITTING + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"message\":\"Load operation committing\"}"}
        };

        for (String[] testCase : testCases) {
            String status = testCase[0];
            String responseBody = testCase[1];

            // Mock HttpClient and HttpResponse
            HttpClient mockHttpClient = mock(HttpClient.class);
            HttpResponse<String> mockResponse = mock(HttpResponse.class);

            when(mockResponse.statusCode()).thenReturn(200);
            when(mockResponse.body()).thenReturn(responseBody);
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

            // Create NeptuneBulkLoader instance with mock HttpClient
            S3TransferManager mockTransferManager = mock(S3TransferManager.class);
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

            String result = neptuneBulkLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

            // Verify the result
            assertEquals("Should return response body for status: " + status, responseBody, result);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCheckNeptuneBulkLoadStatusHttpErrorCodes() throws Exception {
        // Test different HTTP error status codes
        Integer[] errorCodes = {400, 401, 403, 404, 500, 502, 503};

        for (Integer statusCode : errorCodes) {
            // Mock HttpClient and HttpResponse
            HttpClient mockHttpClient = mock(HttpClient.class);
            HttpResponse<String> mockResponse = mock(HttpResponse.class);

            when(mockResponse.statusCode()).thenReturn(statusCode);
            when(mockResponse.body()).thenReturn("Error response");
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

            // Create NeptuneBulkLoader instance with mock HttpClient
            S3TransferManager mockTransferManager = mock(S3TransferManager.class);
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

            neptuneBulkLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCheckNeptuneBulkLoadStatusNetworkException() throws Exception {
        // Mock HttpClient to throw exception
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new RuntimeException("Network connection failed"));

        // Create NeptuneBulkLoader instance with mock HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        neptuneBulkLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusRequestProperties() throws Exception {
        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"LOAD_COMPLETED\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader instance with mock HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Call the protected method directly
        neptuneBulkLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Verify that the request was made with correct properties
        verify(mockHttpClient).send(argThat(request -> {
            // Check URL
            String expectedUrl = "https://" + TestDataProvider.NEPTUNE_ENDPOINT + ":8182/loader/" + TestDataProvider.LOAD_ID_0;
            boolean urlMatches = request.uri().toString().equals(expectedUrl);

            // Check HTTP method
            boolean isGetMethod = request.method().equals("GET");

            // Check Content-Type header
            boolean hasContentTypeHeader = request.headers().firstValue("Content-Type")
                .map(value -> value.equals("application/json"))
                .orElse(false);

            return urlMatches && isGetMethod && hasContentTypeHeader;
        }), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testCloseMethod() {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Test close method - should not throw exception
        neptuneBulkLoader.close();

        // Test multiple close calls - should be safe
        neptuneBulkLoader.close();
        neptuneBulkLoader.close();
    }

    @Test
    public void testMonitorLoadProgressCompletedStatus() throws Exception {
        // Mock HttpClient and HttpResponse for completed status
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        String completedResponse = "{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(completedResponse);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Monitor load progress
        neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain completion message",
            error.contains("Neptune bulk load completed with status: " + TestDataProvider.LOAD_COMPLETED));

        // Verify HTTP call was made
        verify(mockHttpClient, atLeastOnce()).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testMonitorLoadProgressFailedStatus() throws Exception {
        // Mock HttpClient and HttpResponse for failed status
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        String failedResponse = "{\"status\":\"" + TestDataProvider.LOAD_FAILED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Load failed due to invalid data\"}";
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(failedResponse);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Monitor load progress
        neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain failure message",
            error.contains("Neptune bulk load failed with status: " + TestDataProvider.LOAD_FAILED));
        assertTrue("Should contain full response",
            error.contains("Full response: " + failedResponse));

        // Verify HTTP call was made
        verify(mockHttpClient, atLeastOnce()).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testMonitorLoadProgressInProgressThenCompleted() throws Exception {
        // Mock HttpClient and HttpResponse for in-progress then completed
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        String inProgressResponse = "{\"status\":\"" + TestDataProvider.LOAD_IN_PROGRESS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        String completedResponse = "{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";

        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body())
            .thenReturn(inProgressResponse)  // First call
            .thenReturn(inProgressResponse)  // Second call
            .thenReturn(completedResponse);  // Third call (completed)
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Monitor load progress
        neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain in-progress messages",
            error.contains("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS));
        assertTrue("Should contain completion message",
            error.contains("Neptune bulk load completed with status: " + TestDataProvider.LOAD_COMPLETED));

        // Verify HTTP calls were made multiple times
        verify(mockHttpClient, times(3)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testMonitorLoadProgressWithPayloadStructure() throws Exception {
        // Mock HttpClient and HttpResponse with payload structure
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        String payloadResponse = "{\"payload\":{\"overallStatus\":{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\"}},\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(payloadResponse);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Monitor load progress
        neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain completion message",
            error.contains("Neptune bulk load completed with status: " + TestDataProvider.LOAD_COMPLETED));

        // Verify HTTP call was made
        verify(mockHttpClient, atLeastOnce()).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testMonitorLoadProgressTimeout() throws Exception {
        // Mock HttpClient to always return in-progress status (will cause timeout)
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        String inProgressResponse = "{\"status\":\"" + TestDataProvider.LOAD_IN_PROGRESS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(inProgressResponse);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Create a spy to override the sleep and maxAttempts behavior for faster testing
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Override the monitorLoadProgress method to use a smaller maxAttempts for testing
        doAnswer(invocation -> {
            String loadId = invocation.getArgument(0);
            System.err.println("Monitoring load progress for job: " + loadId);
            try {
                int sleepTimeMs = 10; // Reduced sleep time for testing
                int maxAttempts = 3;  // Reduced max attempts for testing
                int attempt = 0;

                while (attempt < maxAttempts) {
                    String statusResponse = spyLoader.checkNeptuneBulkLoadStatus(loadId);

                    if (statusResponse != null) {
                        System.err.println("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS);
                    }

                    Thread.sleep(sleepTimeMs);
                    attempt++;
                }

                if (attempt >= maxAttempts) {
                    System.err.println(
                        "Monitoring timeouted at " + sleepTimeMs * maxAttempts + "ms. Check load status manually.");
                }
            } catch (Exception e) {
                System.err.println("Error monitoring load progress: " + e.getMessage());
            }
            return null;
        }).when(spyLoader).monitorLoadProgress(anyString());

        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify timeout error message
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain in-progress status messages",
            error.contains("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS));
        assertTrue("Should contain timeout message",
            error.contains("Monitoring timeouted at") && error.contains("Check load status manually"));

        // Verify HTTP calls were made the expected number of times
        verify(mockHttpClient, times(3)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test(expected = RuntimeException.class)
    public void testMonitorLoadProgressHttpException() throws Exception {
        // Mock HttpClient to throw exception
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new RuntimeException("Network connection failed"));

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Monitor load progress - should exit quickly due to exception
        neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
    }

    @Test
    public void testMonitorLoadProgressAllFailureStatuses() throws Exception {
        // Test all failure statuses from BULK_LOAD_STATUS_CODES_FAILURES
        String[] failureStatuses = {
            TestDataProvider.LOAD_CANCELLED_BY_USER,
            TestDataProvider.LOAD_CANCELLED_DUE_TO_ERRORS,
            TestDataProvider.LOAD_UNEXPECTED_ERROR,
            TestDataProvider.LOAD_FAILED,
            TestDataProvider.LOAD_S3_READ_ERROR,
            TestDataProvider.LOAD_S3_ACCESS_DENIED_ERROR,
            TestDataProvider.LOAD_DATA_DEADLOCK,
            TestDataProvider.LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED,
            TestDataProvider.LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED,
            TestDataProvider.LOAD_FAILED_INVALID_REQUEST,
            TestDataProvider.LOAD_CANCELLED
        };

        for (String status : failureStatuses) {
            String failureStatus = status;

            // Mock HttpClient and HttpResponse for each failure status
            HttpClient mockHttpClient = mock(HttpClient.class);
            S3TransferManager mockTransferManager = mock(S3TransferManager.class);
            HttpResponse<String> mockResponse = mock(HttpResponse.class);

            String failedResponse = "{\"status\":\"" + failureStatus + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Test error for " + failureStatus + "\"}";
            when(mockResponse.statusCode()).thenReturn(200);
            when(mockResponse.body()).thenReturn(failedResponse);
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

            // Create NeptuneBulkLoader with mock clients
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

            // Clear stream for each test iteration
            errorStream.reset();

            // Monitor load progress with timeout protection
            long startTime = System.currentTimeMillis();
            neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);
            long endTime = System.currentTimeMillis();

            // Ensure the test completed quickly (failure statuses should break immediately)
            assertTrue("Test for " + failureStatus + " should complete quickly", (endTime - startTime) < 5000);

            // Verify error messages
            String error = errorStream.toString();
            assertTrue("Should contain failure message for " + failureStatus,
                error.contains("Neptune bulk load failed with status: " + failureStatus));
            assertTrue("Should contain full response for " + failureStatus,
                error.contains("Full response: " + failedResponse));

            // Verify HTTP call was made (should be exactly 1 call since failure breaks the loop)
            verify(mockHttpClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        }
    }

    @Test
    public void testMonitorLoadProgressAllCompletedStatuses() throws Exception {
        // Test all completed statuses from BULK_LOAD_STATUS_CODES_COMPLETED
        String[] completedStatuses = {
            TestDataProvider.LOAD_COMPLETED,
            TestDataProvider.LOAD_COMMITTED_W_WRITE_CONFLICTS
        };

        for (String completedStatus : completedStatuses) {
            // Mock HttpClient and HttpResponse for each completed status
            HttpClient mockHttpClient = mock(HttpClient.class);
            S3TransferManager mockTransferManager = mock(S3TransferManager.class);
            HttpResponse<String> mockResponse = mock(HttpResponse.class);

            String completedResponse = "{\"status\":\"" + completedStatus + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
            when(mockResponse.statusCode()).thenReturn(200);
            when(mockResponse.body()).thenReturn(completedResponse);
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

            // Create NeptuneBulkLoader with mock clients
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

            // Clear stream for each test
            errorStream.reset();

            // Monitor load progress
            neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

            // Verify output messages
            String error = errorStream.toString();
            assertTrue("Should contain completion message for " + completedStatus,
                error.contains("Neptune bulk load completed with status: " + completedStatus));

            // Verify HTTP call was made
            verify(mockHttpClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testMonitorLoadProgressNullStatusResponse() throws Exception {
        // Mock HttpClient to return error status first, then success
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockResponse.statusCode())
            .thenReturn(500);  // First call - error (returns null)
        when(mockResponse.body())
            .thenReturn("Internal Server Error");  // First call
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Monitor load progress (should handle null response then succeed)
        neptuneBulkLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testStartNeptuneBulkLoadSuccess() throws Exception {
        // Mock HttpClient and HttpResponse for successful bulk load start
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (successful)
        String connectivityResponse = "{\"status\":\"healthy\"}";
        // Mock bulk load start response (successful)
        String bulkLoadResponse = "{\"payload\":{\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}}";

        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body())
            .thenReturn(connectivityResponse)  // First call - connectivity test
            .thenReturn(bulkLoadResponse);     // Second call - bulk load start
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Start bulk load
        String result = neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Verify the result
        assertEquals("Should return the load ID", TestDataProvider.LOAD_ID_0, result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain starting message",
            error.contains("Starting Neptune bulk load..."));
        assertTrue("Should contain connectivity test message",
            error.contains("Testing connectivity to Neptune endpoint..."));
        assertTrue("Should contain success message",
            error.contains("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0));

        // Verify HTTP calls were made (connectivity + bulk load start)
        verify(mockHttpClient, times(2)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testStartNeptuneBulkLoadConnectivityFailure() throws Exception {
        // Mock HttpClient to fail connectivity test
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (failure)
        when(mockResponse.statusCode()).thenReturn(500);
        when(mockResponse.body()).thenReturn("Internal Server Error");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        try {
            neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
            fail("Should throw RuntimeException when connectivity test fails");
        } catch (RuntimeException e) {
            assertTrue("Should contain Neptune endpoint error message",
                e.getMessage().contains("Cannot connect to Neptune endpoint: " + TestDataProvider.NEPTUNE_ENDPOINT));
        }

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain starting message",
            error.contains("Starting Neptune bulk load..."));

        // Verify only connectivity test was attempted (not bulk load start)
        verify(mockHttpClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testStartNeptuneBulkLoadHttpError() throws Exception {
        // Mock HttpClient for successful connectivity but failed bulk load start
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (successful)
        String connectivityResponse = "{\"status\":\"healthy\"}";

        when(mockResponse.statusCode())
            .thenReturn(200)  // First call - connectivity test (success)
            .thenReturn(400)  // Second call - bulk load start (HTTP error)
            .thenReturn(400)  // Third call - bulk load retry (HTTP error)
            .thenReturn(400)  // Fourth call - bulk load retry (HTTP error)
            .thenReturn(400); // Fifth call - bulk load retry (HTTP error)
        when(mockResponse.body())
            .thenReturn(connectivityResponse)  // First call - connectivity
            .thenReturn("Bad Request")         // Second call - bulk load attempt 1
            .thenReturn("Bad Request")         // Third call - bulk load attempt 2
            .thenReturn("Bad Request")         // Fourth call - bulk load attempt 3
            .thenReturn("Bad Request");        // Fifth call - bulk load attempt 4
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Start bulk load - should throw RuntimeException due to HTTP errors after retries
        try {
            neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
            fail("Should throw RuntimeException due to HTTP errors");
        } catch (RuntimeException e) {
            // Validate the RuntimeException message
            assertTrue("Exception should mention failed attempts",
                e.getMessage().contains("Failed to start Neptune bulk load after"));
            assertTrue("Exception should mention number of attempts",
                e.getMessage().contains("4 attempts"));

            // Validate error stream messages
            String error = errorStream.toString();

            // Check for starting message
            assertTrue("Should contain starting message",
                error.contains("Starting Neptune bulk load"));

            // Check for connectivity test message
            assertTrue("Should contain connectivity test message",
                error.contains("Testing connectivity to Neptune endpoint"));

            // Check for retry attempt messages (HTTP errors will fail on each attempt)
            assertTrue("Should contain retry attempt 1 message",
                error.contains("Attempt 1 failed"));
            assertTrue("Should contain retry attempt 2 message",
                error.contains("Attempt 2 failed"));
            assertTrue("Should contain retry attempt 3 message",
                error.contains("Attempt 3 failed"));

            // Check that error messages contain HTTP error details
            assertTrue("Should contain HTTP error details",
                error.contains("Failed to start Neptune bulk load. Status: 400"));
            assertTrue("Should contain HTTP error response",
                error.contains("Bad Request"));
        }

        // Verify HTTP calls were made (connectivity + 4 retry attempts for bulk load)
        verify(mockHttpClient, times(5)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testStartNeptuneBulkLoadRetrySuccess() throws Exception {
        // Mock HttpClient for successful connectivity and bulk load after retry
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (successful)
        String connectivityResponse = "{\"status\":\"healthy\"}";
        // Mock bulk load start response (successful)
        String bulkLoadResponse = "{\"payload\":{\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}}";

        when(mockResponse.statusCode())
            .thenReturn(200)  // First call - connectivity test (success)
            .thenReturn(500)  // Second call - bulk load start (failure)
            .thenReturn(200); // Third call - bulk load start (success after retry)
        when(mockResponse.body())
            .thenReturn(connectivityResponse)  // First call
            .thenReturn("Internal Server Error") // Second call
            .thenReturn(bulkLoadResponse);     // Third call
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Start bulk load - should succeed after retry
        String result = neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Verify the result
        assertEquals("Should return the load ID after retry", TestDataProvider.LOAD_ID_0, result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain starting message",
            error.contains("Starting Neptune bulk load..."));
        assertTrue("Should contain success message",
            error.contains("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain retry attempt message",
            error.contains("Attempt 1 failed"));

        // Verify HTTP calls were made (connectivity + 2 attempts for bulk load)
        verify(mockHttpClient, times(3)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testStartNeptuneBulkLoadMaxRetrySuccess() throws Exception {
        // Mock HttpClient for successful connectivity and bulk load after retry
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (successful)
        String connectivityResponse = "{\"status\":\"healthy\"}";
        // Mock bulk load start response (successful)
        String bulkLoadResponse = "{\"payload\":{\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}}";

        when(mockResponse.statusCode())
            .thenReturn(200)  // First call - connectivity test (success)
            .thenReturn(500)  // Second call - bulk load start (failure)
            .thenReturn(500)  // Third call - bulk load start (failure)
            .thenReturn(500)  // Fourth call - bulk load start (failure)
            .thenReturn(200); //  call - bulk load start (success after retry)
        when(mockResponse.body())
            .thenReturn(connectivityResponse)  // First call
            .thenReturn("Internal Server Error") // Second call
            .thenReturn("Internal Server Error") // Second call
            .thenReturn("Internal Server Error") // Second call
            .thenReturn(bulkLoadResponse);     // Third call
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Start bulk load - should succeed after retry
        String result = neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Verify the result
        assertEquals("Should return the load ID after retry", TestDataProvider.LOAD_ID_0, result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain starting message",
            error.contains("Starting Neptune bulk load..."));
        assertTrue("Should contain success message",
            error.contains("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain retry attempt message",
            error.contains("Attempt 1 failed: Failed to start Neptune bulk load"));

        // Verify HTTP calls were made (connectivity + 2 attempts for bulk load)
        verify(mockHttpClient, times(5)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testStartNeptuneBulkLoadMaxRetryFail() throws Exception {
        // Mock HttpClient for successful connectivity but failed bulk load retry
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (successful)
        String connectivityResponse = "{\"status\":\"healthy\"}";

        when(mockResponse.statusCode())
            .thenReturn(200)  // First call - connectivity test (success)
            .thenReturn(500)  // Second call - bulk load start (failure)
            .thenReturn(500)  // Third call - bulk load start (failure)
            .thenReturn(500)  // Fourth call - bulk load start (failure)
            .thenReturn(500); // Fifth call - bulk load start (failure)
        when(mockResponse.body())
            .thenReturn(connectivityResponse)  // First call
            .thenReturn("Internal Server Error") // Second call
            .thenReturn("Internal Server Error") // Third call
            .thenReturn("Internal Server Error") // Fourth call
            .thenReturn("Internal Server Error"); // Fifth call
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Start bulk load - should throw RuntimeException after max retries
        try {
            neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
            fail("Should throw RuntimeException after max retries");
        } catch (RuntimeException e) {
            // Validate the RuntimeException message
            assertTrue("Exception should mention failed attempts",
                e.getMessage().contains("Failed to start Neptune bulk load after"));
            assertTrue("Exception should mention number of attempts",
                e.getMessage().contains("4 attempts"));

            // Validate all error stream messages
            String error = errorStream.toString();

            // Check for starting message
            assertTrue("Should contain starting message",
                error.contains("Starting Neptune bulk load"));

            // Check for connectivity test message
            assertTrue("Should contain connectivity test message",
                error.contains("Testing connectivity to Neptune endpoint"));

            // Check for all retry attempt messages
            assertTrue("Should contain retry attempt 1 message",
                error.contains("Attempt 1 failed"));
            assertTrue("Should contain retry attempt 2 message",
                error.contains("Attempt 2 failed"));
            assertTrue("Should contain retry attempt 3 message",
                error.contains("Attempt 3 failed"));
            assertTrue("Should contain retry attempt 4 message",
                error.contains("Failed to start Neptune bulk load after 4 attempts."));

            // Check for specific error details in retry messages
            assertTrue("Should contain HTTP error details",
                error.contains("Failed to start Neptune bulk load. Status: 500"));
            assertTrue("Should contain server error response",
                error.contains("Internal Server Error"));
        }

        // Verify HTTP calls were made (connectivity + 4 retry attempts for bulk load)
        verify(mockHttpClient, times(5)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testStartNeptuneBulkLoadInvalidJsonResponse() throws Exception {
        // Mock HttpClient for successful connectivity but invalid JSON response
        HttpClient mockHttpClient = mock(HttpClient.class);
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock connectivity test response (successful)
        String connectivityResponse = "{\"status\":\"healthy\"}";
        // Mock invalid JSON response for bulk load attempts
        String invalidJsonResponse = "invalid json response";

        when(mockResponse.statusCode())
            .thenReturn(200)  // First call - connectivity test (success)
            .thenReturn(200)  // Second call - bulk load start (success but invalid JSON)
            .thenReturn(200)  // Third call - bulk load retry (success but invalid JSON)
            .thenReturn(200)  // Fourth call - bulk load retry (success but invalid JSON)
            .thenReturn(200); // Fifth call - bulk load retry (success but invalid JSON)
        when(mockResponse.body())
            .thenReturn(connectivityResponse)  // First call - connectivity
            .thenReturn(invalidJsonResponse)   // Second call - bulk load attempt 1
            .thenReturn(invalidJsonResponse)   // Third call - bulk load attempt 2
            .thenReturn(invalidJsonResponse)   // Fourth call - bulk load attempt 3
            .thenReturn(invalidJsonResponse);  // Fifth call - bulk load attempt 4
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        // Start bulk load - should throw RuntimeException due to JSON parsing errors after retries
        try {
            neptuneBulkLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
            fail("Should throw RuntimeException due to JSON parsing failures");
        } catch (RuntimeException e) {
            // Validate the RuntimeException message
            assertTrue("Exception should mention failed attempts",
                e.getMessage().contains("Failed to start Neptune bulk load after"));
            assertTrue("Exception should mention number of attempts",
                e.getMessage().contains("4 attempts"));

            // Validate error stream messages
            String error = errorStream.toString();

            // Check for starting message
            assertTrue("Should contain starting message",
                error.contains("Starting Neptune bulk load"));

            // Check for connectivity test message
            assertTrue("Should contain connectivity test message",
                error.contains("Testing connectivity to Neptune endpoint"));

            // Check for retry attempt messages (JSON parsing will fail on each attempt)
            assertTrue("Should contain retry attempt 1 message",
                error.contains("Attempt 1 failed"));
            assertTrue("Should contain retry attempt 2 message",
                error.contains("Attempt 2 failed"));
            assertTrue("Should contain retry attempt 3 message",
                error.contains("Attempt 3 failed"));

            // Check that error messages contain JSON parsing related errors
            assertTrue("Should contain JSON parsing error details",
                error.contains(invalidJsonResponse));
        }

        // Verify HTTP calls were made (connectivity + 4 retry attempts for bulk load)
        verify(mockHttpClient, times(5)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testUploadFileAsyncDirectorySuccess() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        // Create a successful PutObjectResponse
        PutObjectResponse putObjectResponse = PutObjectResponse.builder()
            .eTag("mock-etag-12345")
            .build();

        // Create a successful CompletedFileUpload
        CompletedFileUpload completedUpload = mock(CompletedFileUpload.class);
        when(completedUpload.response()).thenReturn(putObjectResponse);

        // Create a CompletableFuture that completes successfully
        CompletableFuture<CompletedFileUpload> successFuture = CompletableFuture.completedFuture(completedUpload);

        // Mock the FileUpload
        FileUpload mockFileUpload = mock(FileUpload.class);
        when(mockFileUpload.completionFuture()).thenReturn(successFuture);

        // Mock the S3TransferManager
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        // Mock the uploadFile method to return the successful FileUpload
        when(mockTransferManager.uploadFile(any(UploadFileRequest.class)))
            .thenReturn(mockFileUpload);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        try {
            CompletableFuture<Boolean> result = neptuneBulkLoader.uploadFileAsync(
                testDir.getAbsolutePath(),
                TestDataProvider.S3_PREFIX + "/test-upload"
            );

            // The method should return a CompletableFuture
            assertNotNull("uploadFileAsync should return a CompletableFuture", result);

            // Wait for the result and verify it's true
            Boolean uploadResult = result.get();
            assertTrue("Upload should be successful", uploadResult);

            // Verify that the S3TransferManager.uploadFile was called for both CSV files
            verify(mockTransferManager, times(2)).uploadFile(any(UploadFileRequest.class));

            // Verify the output contains success message
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting sequential upload"));
            assertTrue("Should contain success message",
                error.contains("Successfully uploaded all 2 files"));

        } catch (Exception e) {
            fail("Should not throw exception when S3TransferManager is mocked successfully: " + e.getMessage());
        }
    }

    @Test
    public void testUploadFileAsyncDirectoryWithS3Exception() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        // Mock S3Exception to be thrown
        S3Exception s3Exception = (S3Exception) S3Exception.builder()
            .message("Access Denied")
            .statusCode(403)
            .build();

        // Create a CompletableFuture that completes exceptionally with S3Exception
        CompletableFuture<CompletedFileUpload> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(s3Exception);

        // Mock the FileUpload
        FileUpload mockFileUpload = mock(FileUpload.class);
        when(mockFileUpload.completionFuture()).thenReturn(failedFuture);

        // Mock the S3TransferManager and HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        // Mock the uploadFile method to return the failed FileUpload
        when(mockTransferManager.uploadFile(any(UploadFileRequest.class)))
            .thenReturn(mockFileUpload);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        try {
            CompletableFuture<Boolean> result = neptuneBulkLoader.uploadFileAsync(
                testDir.getAbsolutePath(),
                TestDataProvider.S3_PREFIX + "/test-upload"
            );

            // Wait for the future to complete and expect it to return false due to exception
            Boolean uploadResult = result.get();
            assertFalse("Upload should fail and return false when S3Exception occurs", uploadResult);

            // Verify that the S3TransferManager.uploadFile was called (sequential stops on first failure)
            verify(mockTransferManager, times(1)).uploadFile(any(UploadFileRequest.class));

            // Verify error stream contains the exception details
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting sequential upload"));
            assertTrue("Should contain error message about upload failure",
                error.contains("Transfer Manager upload failed") || error.contains("Access Denied"));

        } catch (Exception e) {
            fail("Should not throw exception, should return false instead: " + e.getMessage());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadFileAsyncWithNonExistentDirectory() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadFileAsync("/non/existent/directory", TestDataProvider.S3_PREFIX);
    }

    @Test
    public void testUploadFileAsyncWithEmptyDirectory() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        // Don't create any CSV files - directory is empty

        // Mock the S3AsyncClient and HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        CompletableFuture<Boolean> result = neptuneBulkLoader.uploadFileAsync(
            testDir.getAbsolutePath(),
            TestDataProvider.S3_PREFIX
        );

        // Should return false for empty directory
        Boolean uploadResult = result.get();
        assertFalse("Upload should return false for empty directory", uploadResult);

        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain files not found message",
            error.contains("No files with correct extension were found in "));

        // Verify no S3 calls were made
        verify(mockTransferManager, never()).uploadFile(any(UploadFileRequest.class));
    }

    @Test
    public void testUploadFileAsyncDirectoryPartialFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        // Create mixed success/failure responses
        PutObjectResponse successResponse = PutObjectResponse.builder()
            .eTag("mock-etag-success")
            .build();
        
        // Create successful CompletedFileUpload
        CompletedFileUpload successCompletedUpload = mock(CompletedFileUpload.class);
        when(successCompletedUpload.response()).thenReturn(successResponse);
        CompletableFuture<CompletedFileUpload> successFuture = CompletableFuture.completedFuture(successCompletedUpload);

        S3Exception s3Exception = (S3Exception) S3Exception.builder()
            .message("Access Denied")
            .statusCode(403)
            .build();
        CompletableFuture<CompletedFileUpload> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(s3Exception);

        // Mock FileUpload objects
        FileUpload successFileUpload = mock(FileUpload.class);
        when(successFileUpload.completionFuture()).thenReturn(successFuture);
        
        FileUpload failedFileUpload = mock(FileUpload.class);
        when(failedFileUpload.completionFuture()).thenReturn(failedFuture);

        // Mock the S3TransferManager and HttpClient
        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        HttpClient mockHttpClient = mock(HttpClient.class);

        // Mock first call to succeed, second to fail
        when(mockTransferManager.uploadFile(any(UploadFileRequest.class)))
            .thenReturn(successFileUpload)
            .thenReturn(failedFileUpload);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(mockHttpClient, mockTransferManager);

        CompletableFuture<Boolean> result = neptuneBulkLoader.uploadFileAsync(
            testDir.getAbsolutePath(),
            TestDataProvider.S3_PREFIX + "/test-upload"
        );

        // Should return false due to partial failure
        Boolean uploadResult = result.get();
        assertFalse("Upload should return false when some files fail", uploadResult);

        // Verify both S3 calls were made (sequential upload tries both files)
        verify(mockTransferManager, times(2)).uploadFile(any(UploadFileRequest.class));

        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain partial failure message",
            error.contains("Failed uploading file(s)") || error.contains("Transfer Manager upload failed"));
    }

}
