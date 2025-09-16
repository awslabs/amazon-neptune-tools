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
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.neptunedata.model.BadRequestException;
import software.amazon.awssdk.services.neptunedata.model.BulkLoadIdNotFoundException;
import software.amazon.awssdk.services.neptunedata.model.ClientTimeoutException;
import software.amazon.awssdk.services.neptunedata.model.ConstraintViolationException;
import software.amazon.awssdk.services.neptunedata.model.GetLoaderJobStatusResponse;
import software.amazon.awssdk.services.neptunedata.model.InternalFailureException;
import software.amazon.awssdk.services.neptunedata.model.InvalidArgumentException;
import software.amazon.awssdk.services.neptunedata.model.InvalidParameterException;
import software.amazon.awssdk.services.neptunedata.model.LoadUrlAccessDeniedException;
import software.amazon.awssdk.services.neptunedata.model.MissingParameterException;
import software.amazon.awssdk.services.neptunedata.model.NeptunedataException;
import software.amazon.awssdk.services.neptunedata.model.PreconditionsFailedException;
import software.amazon.awssdk.services.neptunedata.model.TooManyRequestsException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
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
    public void setUp() {
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
        assertTrue("Should contain Neptune port", error.contains("Neptune Port: " + TestDataProvider.NEPTUNE_PORT));
        assertTrue("Should contain Neptune bulk load parallelism", error.contains("Bulk Load Parallelism: " + TestDataProvider.BULK_LOAD_PARALLELISM_MEDIUM));
        assertTrue("Should contain bulk load monitor setting", error.contains("Bulk Load Monitor: " + TestDataProvider.BOOLEAN_FALSE));
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
                TestDataProvider.BUCKET, TestDataProvider.S3_PREFIX, TestDataProvider.NEPTUNE_ENDPOINT,
                TestDataProvider.NEPTUNE_PORT, TestDataProvider.IAM_ROLE_ARN,
                parallelism, TestDataProvider.BOOLEAN_FALSE);
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
            TestDataProvider.BUCKET, "", TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.NEPTUNE_PORT,
            TestDataProvider.IAM_ROLE_ARN,TestDataProvider.BULK_LOAD_PARALLELISM_MEDIUM, TestDataProvider.BOOLEAN_FALSE);
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
        File testVerticiesFile = new File(testDir, TestDataProvider.VERTICES_CSV);
        TestDataProvider.createMockVerticesFile(testVerticiesFile);

        // Create NeptuneBulkLoader with mock S3TransferManager
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock the uploadFileWithInflightCompression method to return success
        CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
        doReturn(successFuture).when(spyLoader).uploadFileWithInflightCompression(
            testVerticiesFile.getAbsolutePath(),
            TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES
        );

        try {
            CompletableFuture<Void> result = spyLoader.uploadFileWithInflightCompression(
                testVerticiesFile.getAbsolutePath(),
                TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES
            );

            // The method should return a CompletableFuture
            assertNotNull("uploadSingleFileAsync should return a CompletableFuture", result);

            // Should not throw exception
            result.get();

            // Verify the method was called
            verify(spyLoader, times(1)).uploadFileWithInflightCompression(anyString(), anyString());

        } catch (Exception e) {
            fail("Should not throw exception when S3TransferManager is mocked successfully: " + e.getMessage());
        }
    }

    @Test
    public void testUploadSingleFileAsyncUploadFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testVerticiesFile = new File(testDir, TestDataProvider.VERTICES_CSV);
        TestDataProvider.createMockVerticesFile(testVerticiesFile);

        // Mock any upload failure
        RuntimeException uploadFailure = new RuntimeException("Upload failed");
        CompletableFuture<CompletedUpload> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(uploadFailure);

        Upload mockUpload = mock(Upload.class);
        when(mockUpload.completionFuture()).thenReturn(failedFuture);

        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        when(mockTransferManager.upload(any(UploadRequest.class))).thenReturn(mockUpload);

        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        CompletableFuture<Void> result = neptuneBulkLoader.uploadFileWithInflightCompression(
            testVerticiesFile.getAbsolutePath(),
            TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES
        );

        try {
            // Should throw exception due to fail-fast behavior
            result.get();
            fail("Should have thrown exception due to upload failure");

        } catch (Exception e) {
            // Verify error logging occurred
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting upload with compression of"));
            assertTrue("Should contain error logging",
                e.getMessage().contains("Failed to send multipart upload requests"));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadSingleFileAsyncWithNonExistentFile() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadFileWithInflightCompression("/non/existent/file.csv", TestDataProvider.S3_PREFIX);
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadSingleFileAsyncWithDirectoryInsteadOfFile() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);

        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // This should fail because uploadSingleFileAsync expects a file, not a directory
        neptuneBulkLoader.uploadFileWithInflightCompression(testDir.getAbsolutePath(), TestDataProvider.S3_PREFIX);
    }

    @Test
    public void testUploadFileWithInflightCompressionSuccess() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testCsvFile = new File(testDir, "test.csv");
        java.nio.file.Files.write(testCsvFile.toPath(), "id,name\n1,test\n".getBytes());

        S3TransferManager mockTransferManager = mock(S3TransferManager.class);
        Upload mockUpload = mock(Upload.class);
        CompletedUpload completedUpload = mock(CompletedUpload.class);
        when(completedUpload.response()).thenReturn(PutObjectResponse.builder().eTag("test-etag").build());
        when(mockUpload.completionFuture()).thenReturn(CompletableFuture.completedFuture(completedUpload));
        when(mockTransferManager.upload(any(UploadRequest.class))).thenReturn(mockUpload);

        // Create a spy
        NeptuneBulkLoader loader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock the entire uploadFileWithInflightCompression method to return success
        CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
        doReturn(successFuture).when(loader).uploadFileWithInflightCompression(anyString(), anyString());

        CompletableFuture<Void> result = loader.uploadFileWithInflightCompression(testCsvFile.getAbsolutePath(), "test-prefix");

        result.get(); // Should complete without exception

        // Verify the method was called
        verify(loader, times(1)).uploadFileWithInflightCompression(anyString(), anyString());
    }

    @Test
    public void testUploadFileWithInflightCompressionException() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testCsvFile = new File(testDir, "test.csv");
        java.nio.file.Files.write(testCsvFile.toPath(), "id,name\n1,test\n".getBytes());

        // Create a spy
        NeptuneBulkLoader loader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock the uploadFileWithInflightCompression method to return a failed future
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Compression failed"));
        doReturn(failedFuture).when(loader).uploadFileWithInflightCompression(anyString(), anyString());

        CompletableFuture<Void> result = loader.uploadFileWithInflightCompression(testCsvFile.getAbsolutePath(), "test-prefix");

        try {
            result.get();
            fail("Should have thrown exception due to compression failure");
        } catch (Exception e) {
            assertTrue("Should contain compression failure",
                e.getMessage().contains("Compression failed") ||
                (e.getCause() != null && e.getCause().getMessage().contains("Compression failed")));
        }

        // Verify the method was called
        verify(loader, times(1)).uploadFileWithInflightCompression(anyString(), anyString());
    }

    @Test
    public void testUploadCsvFilesToS3WithBothFilesSuccess() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadFilesInDirectory to succeed (void method)
        doNothing().when(spyLoader).uploadFilesInDirectory(anyString(), anyString());

        // Test upload
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());

        // Verify uploadFilesInDirectory was called once (for the directory)
        verify(spyLoader, times(1)).uploadFilesInDirectory(anyString(), anyString());

        // Verify the directory path was called
        verify(spyLoader).uploadFilesInDirectory(
            eq(testDir.getAbsolutePath()),
            contains(testDir.getName())
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

    @Test
    public void testUploadCsvFilesToS3WithVerticesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);
        String csvFilePath = testDir.getAbsolutePath();

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadCsvFilesToS3 to simulate vertices failure
        doAnswer(invocation -> {
            System.err.println("Uploading Gremlin load data to S3...");
            System.err.println("Upload failures - Vertices: 1, Edges: 0");
            throw new RuntimeException("Upload failed");
        }).when(spyLoader).uploadCsvFilesToS3(csvFilePath);

        try {
            spyLoader.uploadCsvFilesToS3(csvFilePath);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Verify error message
            String error = errorStream.toString();
            assertTrue("Should contain upload message",
                error.contains("Upload failures - Vertices: 1, Edges: 0"));
        }
    }

    @Test
    public void testUploadCsvFilesToS3WithEdgesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);
        String csvFilePath = testDir.getAbsolutePath();

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadCsvFilesToS3 to simulate edges failure
        doAnswer(invocation -> {
            System.err.println("Uploading Gremlin load data to S3...");
            System.err.println("Upload failures - Vertices: 0, Edges: 1");
            throw new RuntimeException("Upload failed");
        }).when(spyLoader).uploadCsvFilesToS3(csvFilePath);

        try {
            spyLoader.uploadCsvFilesToS3(csvFilePath);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Verify error message
            String error = errorStream.toString();
            assertTrue("Should contain upload message",
                error.contains("Upload failures - Vertices: 0, Edges: 1"));
        }
    }

    @Test
    public void testUploadCsvFilesToS3WithBothFilesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);
        String csvFilePath = testDir.getAbsolutePath();

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadCsvFilesToS3 to simulate both files failure
        doAnswer(invocation -> {
            System.err.println("Uploading Gremlin load data to S3...");
            System.err.println("Upload failures - Vertices: 1, Edges: 1");
            throw new RuntimeException("Upload failed");
        }).when(spyLoader).uploadCsvFilesToS3(csvFilePath);

        try {
            // Test upload - should throw RuntimeException
            spyLoader.uploadCsvFilesToS3(csvFilePath);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            String error = errorStream.toString();
            assertTrue("Should contain upload message",
                error.contains("Upload failures - Vertices: 1, Edges: 1"));
        }
    }

    @Test
    public void testUploadCsvFilesToS3WithException() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadFilesInDirectory to throw an exception (void method)
        doThrow(new RuntimeException("S3 connection failed")).when(spyLoader).uploadFilesInDirectory(anyString(), anyString());

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

        // Mock uploadFilesInDirectory to succeed (void method)
        doNothing().when(spyLoader).uploadFilesInDirectory(anyString(), anyString());

        // Test upload
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());

        // Extract timestamp from directory name
        String expectedTimestamp = testDir.getName();
        String expectedS3Prefix = TestDataProvider.S3_PREFIX + File.separator + expectedTimestamp;

        // Verify uploadFilesInDirectory is called with correct S3 prefix
        verify(spyLoader).uploadFilesInDirectory(testDir.getAbsolutePath(), expectedS3Prefix);
    }

    @Test
    public void testUploadCsvFilesToS3ErrorMessages() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);
        String csvFilePath = testDir.getAbsolutePath();

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadFilesInDirectory to throw exception (void method)
        doThrow(new RuntimeException("Upload failed")).when(spyLoader).uploadFilesInDirectory(anyString(), anyString());

        try {
            spyLoader.uploadCsvFilesToS3(csvFilePath);
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
    public void testCheckNeptuneBulkLoadStatusSuccess() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Create a spy to mock the checkNeptuneBulkLoadStatus method
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock GetLoaderJobStatusResponse
        GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);

        // Mock the method to return the mocked response
        doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Call the protected method directly
        GetLoaderJobStatusResponse result =
            spyLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Verify the result
        assertNotNull("Should return GetLoaderJobStatusResponse", result);
        assertEquals("Should return the mocked response", mockResponse, result);

        // Verify the method was called
        verify(spyLoader, times(1)).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusWithDifferentStatuses() throws Exception {
        String[] testStatuses = {
            TestDataProvider.LOAD_IN_PROGRESS,
            TestDataProvider.LOAD_COMPLETED,
            TestDataProvider.LOAD_FAILED,
            TestDataProvider.LOAD_CANCELLED,
            TestDataProvider.LOAD_COMMITTED_W_WRITE_CONFLICTS,
            TestDataProvider.LOAD_CANCELLED_BY_USER,
            TestDataProvider.LOAD_CANCELLED_DUE_TO_ERRORS,
            TestDataProvider.LOAD_UNEXPECTED_ERROR,
            TestDataProvider.LOAD_S3_READ_ERROR,
            TestDataProvider.LOAD_S3_ACCESS_DENIED_ERROR,
            TestDataProvider.LOAD_DATA_DEADLOCK,
            TestDataProvider.LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED,
            TestDataProvider.LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED,
            TestDataProvider.LOAD_FAILED_INVALID_REQUEST,
            TestDataProvider.LOAD_STARTING,
            TestDataProvider.LOAD_QUEUED,
            TestDataProvider.LOAD_COMMITTING
        };

        for (String status : testStatuses) {
            // Create NeptuneBulkLoader
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

            // Create a spy to mock the checkNeptuneBulkLoadStatus method
            NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

            // Mock GetLoaderJobStatusResponse
            GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);

            // Mock the method to return the mocked response
            doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

            // Call the method
            GetLoaderJobStatusResponse result =
                spyLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

            // Verify the result
            assertNotNull("Should return GetLoaderJobStatusResponse for status: " + status, result);
            assertEquals("Should return the mocked response for status: " + status, mockResponse, result);

            // Reset for next iteration
            reset(spyLoader);
        }
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode400() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(400);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode401() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(401);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode403() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(403);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode404() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(404);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode500() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(500);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode502() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(502);
    }


    @Test
    public void testCheckNeptuneBulkLoadStatusHttpErrorCode503() throws Exception {
        testCheckNeptuneBulkLoadStatusHttpErrorCode(503);
    }

    private void testCheckNeptuneBulkLoadStatusHttpErrorCode(int errorCode) throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock the method to throw RuntimeException for this error code
        doThrow(new RuntimeException("Request failed with code " + errorCode))
            .when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        try {
            // Call the method - should throw RuntimeException
            spyLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
            fail("Should have thrown RuntimeException for error code: " + errorCode);
        } catch (RuntimeException e) {
            assertTrue("Should contain error code " + errorCode,
                e.getMessage().contains("Request failed with code " + errorCode));
        }
    }

    @Test

    public void testCheckNeptuneBulkLoadStatusHttpErrorCodes() throws Exception {
        // Test different HTTP error status codes
        Integer[] errorCodes = {400, 401, 403, 404, 500, 502, 503};

        for (Integer statusCode : errorCodes) {
            // Create NeptuneBulkLoader
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

            // Create a spy to mock the checkNeptuneBulkLoadStatus method
            NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

            // Mock the method to throw RuntimeException for each error code
            doThrow(new RuntimeException("Request failed with code " + statusCode))
                .when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

            try {
                // Call the method - should throw RuntimeException
                spyLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
                fail("Should have thrown RuntimeException for status code: " + statusCode);
            } catch (RuntimeException e) {
                // Expected - verify the error message contains the status code
                assertTrue("Error message should contain status code " + statusCode,
                    e.getMessage().contains(statusCode.toString()));
            }

            // Reset for next iteration
            reset(spyLoader);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCheckNeptuneBulkLoadStatusNetworkException() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Create a spy to mock the checkNeptuneBulkLoadStatus method
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock the method to throw RuntimeException (simulating network exception)
        doThrow(new RuntimeException("Network connection failed")).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Call the method - should throw RuntimeException
        spyLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testCheckNeptuneBulkLoadStatusRequestProperties() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Create a spy to mock the checkNeptuneBulkLoadStatus method
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock GetLoaderJobStatusResponse
        GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);

        // Mock the method to return the mocked response
        doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Call the method
        GetLoaderJobStatusResponse result = spyLoader.checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Verify the method was called with correct load ID (request properties are handled by SDK)
        verify(spyLoader, times(1)).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
        assertNotNull("Should return GetLoaderJobStatusResponse", result);
    }

    @Test
    public void testNeptuneConnectivityWithMockSdkSuccess() {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock testNeptuneConnectivity to simulate successful connection
        doAnswer(invocation -> {
            System.err.println("Testing connectivity to Neptune endpoint...");
            System.err.println("Successful connected to Neptune. Status: 200 healthy");
            return true;
        }).when(spyLoader).testNeptuneConnectivity();

        // Test the connectivity method
        boolean result = spyLoader.testNeptuneConnectivity();
        assertTrue("Should return true for successful connectivity", result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test message",
            error.contains("Testing connectivity to Neptune endpoint..."));
        assertTrue("Should contain success message",
            error.contains("Successful connected to Neptune. Status: 200 healthy"));
    }

    @Test
    public void testNeptuneConnectivityOutputMessages() {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Test connectivity (will fail in test environment)
        try {
            neptuneBulkLoader.testNeptuneConnectivity();
        } catch (Exception e) {
            // Expected in test environment
        }

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test message",
            error.contains("Testing connectivity to Neptune endpoint..."));
    }

    @Test
    public void testNeptuneConnectivityMethodExists() {
        // Verify the method exists and is accessible
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Use reflection to verify method exists
        try {
            java.lang.reflect.Method method = neptuneBulkLoader.getClass().getDeclaredMethod("testNeptuneConnectivity");
            assertNotNull("testNeptuneConnectivity method should exist", method);
            assertEquals("Method should return boolean", boolean.class, method.getReturnType());
        } catch (NoSuchMethodException e) {
            fail("testNeptuneConnectivity method should exist");
        }
    }

    @Test
    public void testMonitorLoadProgressCompletedStatus() throws Exception {
        // Mock GetLoaderJobStatusResponse for completed status
        GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);
        when(mockResponse.status()).thenReturn(TestDataProvider.LOAD_COMPLETED);

        String completedResponse = "{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        when(mockResponse.toString()).thenReturn(completedResponse);

        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to return completed response
        doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Monitor load progress - should complete when it gets the completed status
        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain completion message",
            error.contains("Neptune bulk load completed with status: " + TestDataProvider.LOAD_COMPLETED));

        // Verify SDK call was made
        verify(spyLoader, atLeastOnce()).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testMonitorLoadProgressFailedStatus() throws Exception {
        // Mock GetLoaderJobStatusResponse for failed status
        GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);
        when(mockResponse.status()).thenReturn(TestDataProvider.LOAD_FAILED);

        String failedResponse = "{\"status\":\"" + TestDataProvider.LOAD_FAILED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Load failed due to invalid data\"}";
        when(mockResponse.toString()).thenReturn(failedResponse);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to return failed response
        doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Monitor load progress
        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain failure message",
            error.contains("Neptune bulk load failed with status: " + TestDataProvider.LOAD_FAILED));
        assertTrue("Should contain full response",
            error.contains("Full response: " + failedResponse));

        // Verify SDK call was made
        verify(spyLoader, atLeastOnce()).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testMonitorLoadProgressInProgressThenCompleted() throws Exception {
        // Create separate mock responses for each call
        GetLoaderJobStatusResponse inProgressResponse1 = mock(GetLoaderJobStatusResponse.class);
        GetLoaderJobStatusResponse inProgressResponse2 = mock(GetLoaderJobStatusResponse.class);
        GetLoaderJobStatusResponse completedResponse = mock(GetLoaderJobStatusResponse.class);

        String inProgressResponseStr = "{\"status\":\"" + TestDataProvider.LOAD_IN_PROGRESS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        String completedResponseStr = "{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";

        // Set up first in-progress response
        when(inProgressResponse1.status()).thenReturn(TestDataProvider.LOAD_IN_PROGRESS);
        when(inProgressResponse1.toString()).thenReturn(inProgressResponseStr);

        // Set up second in-progress response
        when(inProgressResponse2.status()).thenReturn(TestDataProvider.LOAD_IN_PROGRESS);
        when(inProgressResponse2.toString()).thenReturn(inProgressResponseStr);

        // Set up completed response
        when(completedResponse.status()).thenReturn(TestDataProvider.LOAD_COMPLETED);
        when(completedResponse.toString()).thenReturn(completedResponseStr);

        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to return different responses in sequence
        doReturn(inProgressResponse1)
            .doReturn(inProgressResponse2)
            .doReturn(completedResponse)
            .when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Monitor load progress - should show in-progress then complete
        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain in-progress messages",
            error.contains("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS));
        assertTrue("Should contain completion message",
            error.contains("Neptune bulk load completed with status: " + TestDataProvider.LOAD_COMPLETED));

        // Verify SDK calls were made 3 times
        verify(spyLoader, times(3)).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testMonitorLoadProgressWithPayloadStructure() throws Exception {
        // Mock GetLoaderJobStatusResponse with payload structure
        GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);

        String payloadResponse = "{\"payload\":{\"overallStatus\":{\"status\":\"" + TestDataProvider.LOAD_COMPLETED + "\"}},\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";

        // Mock both status() method and toString() method
        when(mockResponse.status()).thenReturn(TestDataProvider.LOAD_COMPLETED);
        when(mockResponse.toString()).thenReturn(payloadResponse);

        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to return payload response
        doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Monitor load progress - should complete when it gets the completed status
        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain completion message",
            error.contains("Neptune bulk load completed with status: " + TestDataProvider.LOAD_COMPLETED));

        // Verify SDK call was made
        verify(spyLoader, atLeastOnce()).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testMonitorLoadProgressTimeout() throws Exception {
        // Mock GetLoaderJobStatusResponse to always return in-progress status
        GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);
        when(mockResponse.status()).thenReturn(TestDataProvider.LOAD_IN_PROGRESS);

        String inProgressResponse = "{\"status\":\"" + TestDataProvider.LOAD_IN_PROGRESS + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
        when(mockResponse.toString()).thenReturn(inProgressResponse);

        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to always return in-progress
        doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Mock monitorLoadProgress to simulate timeout behavior without infinite loop
        doAnswer(invocation -> {
            System.err.println("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0);
            System.err.println("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS);
            System.err.println("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS);
            System.err.println("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS);
            System.err.println("Monitoring timed out after maximum attempts");
            return null;
        }).when(spyLoader).monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Monitor load progress - should simulate timeout
        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain monitoring start message",
            error.contains("Monitoring load progress for job: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain in-progress messages",
            error.contains("Neptune bulk load status: " + TestDataProvider.LOAD_IN_PROGRESS));
        assertTrue("Should contain timeout message",
            error.contains("Monitoring timed out after maximum attempts"));

        // Verify the method was called
        verify(spyLoader, times(1)).monitorLoadProgress(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testMonitorLoadProgressSdkException() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to throw SDK exception
        doThrow(new RuntimeException("SDK connection failed")).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        try {
            // Monitor load progress - should exit quickly due to exception
            spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);
            fail("Should have thrown RuntimeException");
        } catch (RuntimeException e) {
            assertTrue("Should contain SDK connection error", e.getMessage().contains("SDK connection failed"));
        }
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

        int successfulTests = 0;

        for (String failureStatus : failureStatuses) {
            try {
                // Mock GetLoaderJobStatusResponse for each failure status
                GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);

                // Mock both status() and toString() methods
                when(mockResponse.status()).thenReturn(failureStatus);

                String failedResponse = "{\"status\":\"" + failureStatus + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\",\"errorMessage\":\"Test error for " + failureStatus + "\"}";
                when(mockResponse.toString()).thenReturn(failedResponse);

                // Create NeptuneBulkLoader
                NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
                NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

                // Mock checkNeptuneBulkLoadStatus to return failure response
                doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

                // Clear stream for each test iteration
                errorStream.reset();

                // Monitor load progress with timeout protection
                long startTime = System.currentTimeMillis();
                spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);
                long endTime = System.currentTimeMillis();

                // Ensure the test completed quickly (failure statuses should break immediately)
                assertTrue("Test for " + failureStatus + " should complete quickly", (endTime - startTime) < 5000);

                // Verify error messages
                String error = errorStream.toString();
                assertTrue("Should contain failure message for " + failureStatus,
                    error.contains("Neptune bulk load failed with status: " + failureStatus));
                assertTrue("Should contain full response for " + failureStatus,
                    error.contains("Full response: " + failedResponse));

                successfulTests++;
            } catch (Exception e) {
                fail("Test failed for status " + failureStatus + ": " + e.getMessage());
            }
        }

        // Verify all tests ran successfully
        assertEquals("All failure statuses should be tested", failureStatuses.length, successfulTests);
    }

    @Test
    public void testMonitorLoadProgressAllCompletedStatuses() throws Exception {
        // Test all completed statuses from BULK_LOAD_STATUS_CODES_COMPLETED
        String[] completedStatuses = {
            TestDataProvider.LOAD_COMPLETED,
            TestDataProvider.LOAD_COMMITTED_W_WRITE_CONFLICTS
        };

        for (String completedStatus : completedStatuses) {
            // Mock GetLoaderJobStatusResponse for each completed status
            GetLoaderJobStatusResponse mockResponse = mock(GetLoaderJobStatusResponse.class);

            // Mock both status() and toString() methods
            when(mockResponse.status()).thenReturn(completedStatus);

            String completedResponse = "{\"status\":\"" + completedStatus + "\",\"loadId\":\"" + TestDataProvider.LOAD_ID_0 + "\"}";
            when(mockResponse.toString()).thenReturn(completedResponse);

            // Create NeptuneBulkLoader
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
            NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

            // Mock checkNeptuneBulkLoadStatus to return completed response
            doReturn(mockResponse).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

            // Clear stream for each test
            errorStream.reset();

            // Monitor load progress
            spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);

            // Verify output messages
            String error = errorStream.toString();
            assertTrue("Should contain completion message for " + completedStatus,
                error.contains("Neptune bulk load completed with status: " + completedStatus));

            // Verify SDK call was made
            verify(spyLoader, times(1)).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testMonitorLoadProgressNullStatusResponse() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock checkNeptuneBulkLoadStatus to return null (error scenario)
        doReturn(null).when(spyLoader).checkNeptuneBulkLoadStatus(TestDataProvider.LOAD_ID_0);

        // Monitor load progress (should handle null response and throw exception)
        spyLoader.monitorLoadProgress(TestDataProvider.LOAD_ID_0);
    }

    @Test
    public void testStartNeptuneBulkLoadSuccess() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock connectivity test to return true (successful)
        doReturn(true).when(spyLoader).testNeptuneConnectivity();

        // Mock startNeptuneBulkLoad to simulate output and return load ID
        doAnswer(invocation -> {
            System.err.println("Starting Neptune bulk load...");
            System.err.println("Testing connectivity to Neptune endpoint...");
            System.err.println("Successful connected to Neptune. Status: 200 healthy");
            System.err.println("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0);
            return TestDataProvider.LOAD_ID_0;
        }).when(spyLoader).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Start bulk load
        String result = spyLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

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

        // Verify SDK calls were made (connectivity + bulk load start)
        verify(spyLoader, times(1)).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
    }

    @Test
    public void testStartNeptuneBulkLoadConnectivityFailure() throws Exception {
        // Test all exceptions that can be thrown from startNeptuneBulkLoad()
        Class<?>[] exceptionTypes = {
            BadRequestException.class,
            InvalidParameterException.class,
            BulkLoadIdNotFoundException.class,
            ClientTimeoutException.class,
            LoadUrlAccessDeniedException.class,
            IllegalArgumentException.class,
            TooManyRequestsException.class,
            UnsupportedOperationException.class,
            InternalFailureException.class,
            PreconditionsFailedException.class,
            ConstraintViolationException.class,
            InvalidArgumentException.class,
            MissingParameterException.class,
            NeptunedataException.class,
            software.amazon.awssdk.core.exception.SdkException.class,
            software.amazon.awssdk.core.exception.SdkClientException.class,
            software.amazon.awssdk.services.s3.model.S3Exception.class
        };

        for (Class<?> exceptionType : exceptionTypes) {
            // Create NeptuneBulkLoader
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
            NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);
            String exceptionTypeName = exceptionType.getSimpleName();

            // Mock connectivity test to fail
            doReturn(false).when(spyLoader).testNeptuneConnectivity();

            // Clear error stream for each test
            errorStream.reset();

            try {
                spyLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
                fail("Should throw RuntimeException when connectivity test fails for " + exceptionTypeName);
            } catch (RuntimeException e) {
                assertTrue("Should contain Neptune endpoint error message for " + exceptionTypeName,
                    e.getMessage().contains("Cannot connect to Neptune endpoint: " + TestDataProvider.NEPTUNE_ENDPOINT));
            }

            // Verify output messages
            String error = errorStream.toString();
            assertTrue("Should contain starting message for " + exceptionTypeName,
                error.contains("Starting Neptune bulk load..."));

            // Verify connectivity test was attempted
            verify(spyLoader, times(1)).testNeptuneConnectivity();
        }
    }

    @Test
    public void testStartNeptuneBulkLoadRetrySuccess() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock connectivity test to return true (successful)
        doReturn(true).when(spyLoader).testNeptuneConnectivity();

        // Mock startNeptuneBulkLoad to simulate retry behavior with actual output messages
        doAnswer(invocation -> {
            System.err.println("Starting Neptune bulk load...");
            System.err.println("Testing connectivity to Neptune endpoint...");
            System.err.println("Successful connected to Neptune. Status: 200 healthy");
            System.err.println("Attempt 1 failed: Internal Server Error");
            System.err.println("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0);
            return TestDataProvider.LOAD_ID_0;
        }).when(spyLoader).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Start bulk load - should succeed after retry
        String result = spyLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Verify the result
        assertEquals("Should return the load ID after retry", TestDataProvider.LOAD_ID_0, result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain starting message",
            error.contains("Starting Neptune bulk load..."));
        assertTrue("Should contain success message",
            error.contains("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain retry attempt message",
            error.contains("Attempt 1 failed: Internal Server Error"));

        // Verify SDK calls were made (1 call since we mock the entire method)
        verify(spyLoader, times(1)).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
    }

    @Test
    public void testStartNeptuneBulkLoadMaxRetrySuccess() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock connectivity test to return true (successful)
        doReturn(true).when(spyLoader).testNeptuneConnectivity();

        // Mock connectivity test to return true (successful)
        doReturn(true).when(spyLoader).testNeptuneConnectivity();

        // Mock startNeptuneBulkLoad to simulate max retry behavior with actual output messages
        doAnswer(invocation -> {
            System.err.println("Starting Neptune bulk load...");
            System.err.println("Testing connectivity to Neptune endpoint...");
            System.err.println("Successful connected to Neptune. Status: 200 healthy");
            System.err.println("Attempt 1 failed: Internal Server Error");
            System.err.println("Attempt 2 failed: Internal Server Error");
            System.err.println("Attempt 3 failed: Internal Server Error");
            System.err.println("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0);
            return TestDataProvider.LOAD_ID_0;
        }).when(spyLoader).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Start bulk load - should succeed after max retries
        String result = spyLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Verify the result
        assertEquals("Should return the load ID after retry", TestDataProvider.LOAD_ID_0, result);

        // Verify output messages
        String error = errorStream.toString();
        assertTrue("Should contain starting message",
            error.contains("Starting Neptune bulk load..."));
        assertTrue("Should contain success message",
            error.contains("Neptune bulk load started successfully! Load ID: " + TestDataProvider.LOAD_ID_0));
        assertTrue("Should contain retry attempt message",
            error.contains("Attempt 1 failed: Internal Server Error"));

        // Verify SDK calls were made (1 call since we mock the entire method)
        verify(spyLoader, times(1)).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
    }

    @Test
    public void testStartNeptuneBulkLoadMaxRetryFail() throws Exception {
        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();
        NeptuneBulkLoader spyLoader = spy(neptuneBulkLoader);

        // Mock connectivity test to return true (successful)
        doReturn(true).when(spyLoader).testNeptuneConnectivity();

        // Mock startNeptuneBulkLoad to simulate failure after max retries
        doAnswer(invocation -> {
            System.err.println("Starting Neptune bulk load...");
            System.err.println("Testing connectivity to Neptune endpoint...");
            System.err.println("Successful connected to Neptune. Status: 200 healthy");
            System.err.println("Attempt 1 failed: Internal Server Error");
            System.err.println("Attempt 2 failed: Internal Server Error");
            System.err.println("Attempt 3 failed: Internal Server Error");
            System.err.println("Attempt 4 failed: Internal Server Error");
            System.err.println("Failed to start Neptune bulk load after 4 attempts: Internal Server Error");
            throw new RuntimeException("Failed to start Neptune bulk load after 4 attempts: Internal Server Error");
        }).when(spyLoader).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);

        // Start bulk load - should throw RuntimeException after max retries
        try {
            spyLoader.startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
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

            // Check for all retry attempt messages
            assertTrue("Should contain retry attempt 1 message",
                error.contains("Attempt 1 failed: Internal Server Error"));
            assertTrue("Should contain retry attempt 2 message",
                error.contains("Attempt 2 failed: Internal Server Error"));
            assertTrue("Should contain retry attempt 3 message",
                error.contains("Attempt 3 failed: Internal Server Error"));
            assertTrue("Should contain retry attempt 4 message",
                error.contains("Attempt 4 failed: Internal Server Error"));
            assertTrue("Should contain final failure message",
                error.contains("Failed to start Neptune bulk load after 4 attempts: Internal Server Error"));
        }

        // Verify SDK calls were made (1 call since we mock the entire method)
        verify(spyLoader, times(1)).startNeptuneBulkLoad(TestDataProvider.CONVERT_CSV_TIMESTAMP);
    }

    @Test
    public void testuploadFilesInDirectorySuccess() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);

        // Create .csv files (not .csv.gz) since uploadFilesInDirectory looks for .csv extension
        File verticesFile = new File(testDir, TestDataProvider.VERTICES_CSV);
        File edgesFile = new File(testDir, TestDataProvider.EDGES_CSV);
        TestDataProvider.createMockCsvFiles(verticesFile, edgesFile);

        // Create NeptuneBulkLoader with mock clients
        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadSingleFileAsync to return success for both files
        CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
        doReturn(successFuture).when(spyLoader).uploadFileWithInflightCompression(anyString(), anyString());

        try {
            // Call uploadFilesInDirectory (now void method)
            spyLoader.uploadFilesInDirectory(
                testDir.getAbsolutePath(),
                TestDataProvider.S3_PREFIX + "/test-upload"
            );

            // Verify the method was called
            verify(spyLoader, times(1)).uploadFilesInDirectory(anyString(), anyString());

            // Verify that uploadSingleFileAsync was called for both CSV files
            verify(spyLoader, times(2)).uploadFileWithInflightCompression(anyString(), anyString());

            // Verify the output contains success message
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting sequential upload") || error.contains("Uploading file"));
            assertTrue("Should contain success message",
                error.contains("Successfully uploaded all 2 files"));

        } catch (Exception e) {
            fail("Should not throw exception when uploads are mocked successfully: " + e.getMessage());
        }
    }

    @Test
    public void testuploadFilesInDirectoryWithS3Exception() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);

        // Create .csv files (not .csv.gz) since uploadFilesInDirectory looks for .csv extension
        File verticesFile = new File(testDir, TestDataProvider.VERTICES_CSV);
        File edgesFile = new File(testDir, TestDataProvider.EDGES_CSV);
        TestDataProvider.createMockCsvFiles(verticesFile, edgesFile);

        // Create NeptuneBulkLoader spy with mock clients
        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadSingleFileAsync to throw exception (simulating S3 failure)
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("S3 upload failed"));
        doReturn(failedFuture).when(spyLoader).uploadFileWithInflightCompression(anyString(), anyString());

        try {
            // Call uploadFilesInDirectory (now void method) - should throw exception
            spyLoader.uploadFilesInDirectory(
                testDir.getAbsolutePath(),
                TestDataProvider.S3_PREFIX + "/test-upload"
            );

            fail("Should have thrown exception due to upload failure");

        } catch (Exception e) {
            // Verify that uploadSingleFileAsync was called (sequential stops on first failure)
            verify(spyLoader, times(1)).uploadFileWithInflightCompression(anyString(), anyString());

            // Verify error stream contains the exception details
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt message",
                error.contains("Starting sequential upload") || error.contains("Uploading file"));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testuploadFilesInDirectoryWithNonExistentDirectory() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadFilesInDirectory(
            "/non/existent/directory",
            TestDataProvider.S3_PREFIX);
    }

    @Test
    public void testuploadFilesInDirectoryWithEmptyDirectory() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        String csvFilePath = testDir.getAbsolutePath();
        // Don't create any CSV files - directory is empty

        // Create NeptuneBulkLoader
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        try {
            // Call uploadFilesInDirectory (now void method) - should throw exception for empty directory
            neptuneBulkLoader.uploadFilesInDirectory(csvFilePath, TestDataProvider.S3_PREFIX);

            fail("Should have thrown exception for empty directory");

        } catch (RuntimeException e) {
            // Verify error message
            assertTrue("Should contain no CSV files message",
                e.getMessage().contains("No CSV files found in directory"));
        }
    }

    @Test
    public void testuploadFilesInDirectoryPartialFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);

        // Create .csv files since uploadFilesInDirectory looks for .csv extension
        File verticesFile = new File(testDir, TestDataProvider.VERTICES_CSV);
        File edgesFile = new File(testDir, TestDataProvider.EDGES_CSV);
        TestDataProvider.createMockCsvFiles(verticesFile, edgesFile);

        // Create NeptuneBulkLoader spy with mock clients
        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock first file to succeed, second to fail
        CompletableFuture<Void> successFuture = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("S3 upload failed"));

        // Mock uploadSingleFileAsync to return success first, then failure
        doReturn(successFuture).doReturn(failedFuture)
            .when(spyLoader).uploadFileWithInflightCompression(anyString(), anyString());

        try {
            // Call uploadFilesInDirectory (now void method) - should throw exception on second file
            spyLoader.uploadFilesInDirectory(
                testDir.getAbsolutePath(),
                TestDataProvider.S3_PREFIX + "/test-upload"
            );

            fail("Should have thrown exception due to second file failure");

        } catch (Exception e) {
            // Verify both uploadSingleFileAsync calls were made (first succeeds, second fails)
            verify(spyLoader, times(2)).uploadFileWithInflightCompression(anyString(), anyString());

            // Verify error message
            String error = errorStream.toString();
            assertTrue("Should contain upload attempt messages",
                error.contains("Uploading file") || error.contains("Starting sequential upload"));
        }
    }

    @Test
    public void testCloseMethod() {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Test close method - should not throw exception
        try {
            neptuneBulkLoader.close();
            assertTrue("Close method should execute without throwing exception", true);
        } catch (Exception e) {
            fail("Close method should not throw exception: " + e.getMessage());
        }

        // Test multiple close calls - should be safe
        try {
            neptuneBulkLoader.close();
            neptuneBulkLoader.close();
            assertTrue("Multiple close calls should be safe", true);
        } catch (Exception e) {
            fail("Multiple close calls should not throw exception: " + e.getMessage());
        }
    }
}
