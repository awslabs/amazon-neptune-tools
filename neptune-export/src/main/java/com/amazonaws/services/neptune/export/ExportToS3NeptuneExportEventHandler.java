/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.export;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.neptune.util.TransferManagerWrapper;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazonaws.services.neptune.export.NeptuneExportService.NEPTUNE_EXPORT_TAGS;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ExportToS3NeptuneExportEventHandler implements NeptuneExportEventHandler {

    public static class S3UploadParams {
        private boolean createExportSubdirectory = true;
        private boolean overwriteExisting = false;

        public boolean createExportSubdirectory() {
            return createExportSubdirectory;
        }

        public S3UploadParams setCreateExportSubdirectory(boolean createExportSubdirectory) {
            this.createExportSubdirectory = createExportSubdirectory;
            return this;
        }

        public boolean overwriteExisting() {
            return overwriteExisting;
        }

        public S3UploadParams setOverwriteExisting(boolean overwriteExisting) {
            this.overwriteExisting = overwriteExisting;
            return this;
        }

        @Override
        public String toString() {
            return "{" +
                    "createExportSubdirectory=" + createExportSubdirectory +
                    ", overwriteExisting=" + overwriteExisting +
                    '}';
        }
    }

    public static ObjectTagging createObjectTags(Collection<String> profiles) {
        List<Tag> tags = new ArrayList<>(NEPTUNE_EXPORT_TAGS);
        if (!profiles.isEmpty()) {
            String profilesTagValue = String.join(":", profiles);
            tags.add(new Tag("neptune-export:profiles", profilesTagValue));
        }
        return new ObjectTagging(tags);
    }

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ExportToS3NeptuneExportEventHandler.class);

    private final String localOutputPath;
    private final String outputS3Path;
    private final String s3Region;
    private final String completionFileS3Path;
    private final ObjectNode completionFilePayload;
    private final boolean uploadToS3OnError;
    private final S3UploadParams s3UploadParams;
    private final Collection<String> profiles;
    private final Collection<CompletionFileWriter> completionFileWriters;
    private final AtomicReference<S3ObjectInfo> result = new AtomicReference<>();

    public ExportToS3NeptuneExportEventHandler(String localOutputPath,
                                               String outputS3Path,
                                               String s3Region,
                                               String completionFileS3Path,
                                               ObjectNode completionFilePayload,
                                               boolean uploadToS3OnError,
                                               S3UploadParams s3UploadParams,
                                               Collection<String> profiles,
                                               Collection<CompletionFileWriter> completionFileWriters) {
        this.localOutputPath = localOutputPath;
        this.outputS3Path = outputS3Path;
        this.s3Region = s3Region;
        this.completionFileS3Path = completionFileS3Path;
        this.completionFilePayload = completionFilePayload;
        this.uploadToS3OnError = uploadToS3OnError;
        this.s3UploadParams = s3UploadParams;
        this.profiles = profiles;
        this.completionFileWriters = completionFileWriters;
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster) throws Exception {
        onExportComplete(directories, stats, cluster, new GraphSchema());
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster, GraphSchema graphSchema) throws Exception {

        try {
            long size = Files.walk(directories.rootDirectory()).mapToLong(p -> p.toFile().length()).sum();
            logger.info("Total size of exported files: {}", FileUtils.byteCountToDisplaySize(size));
        } catch (Exception e) {
            // Ignore
        }

        if (StringUtils.isEmpty(outputS3Path)) {
            return;
        }

        logger.info("S3 upload params: {}", s3UploadParams);

        try (TransferManagerWrapper transferManager = new TransferManagerWrapper(s3Region)) {

            File outputDirectory = directories.rootDirectory().toFile();
            S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory);

            Timer.timedActivity("uploading files to S3", (CheckedActivity.Runnable) () -> {
                deleteS3Directories(directories, outputS3ObjectInfo);
                uploadExportFilesToS3(transferManager.get(), outputDirectory, outputS3ObjectInfo);
                uploadCompletionFileToS3(transferManager.get(), outputDirectory, outputS3ObjectInfo, stats, graphSchema);
            });

            result.set(outputS3ObjectInfo);
        }
    }

    public S3ObjectInfo result() {
        return result.get();
    }

    @Override
    public void onError() {

        if (!uploadToS3OnError) {
            return;
        }

        logger.warn("Uploading results of failed export to S3");

        if (StringUtils.isEmpty(outputS3Path)) {
            logger.warn("S3 output path is empty");
            return;
        }

        try {
            Path outputPath = Paths.get(localOutputPath);

            long size = Files.walk(outputPath).mapToLong(p -> p.toFile().length()).sum();
            logger.warn("Total size of failed export files: {}", FileUtils.byteCountToDisplaySize(size));

            try (TransferManagerWrapper transferManager = new TransferManagerWrapper(s3Region)) {

                String s3Suffix = UUID.randomUUID().toString().replace("-", "");

                File outputDirectory = outputPath.toFile();
                S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory)
                        .replaceOrAppendKey("/tmp", "/failed")
                        .withNewKeySuffix(s3Suffix);

                Timer.timedActivity("uploading failed export files to S3", (CheckedActivity.Runnable) () -> {
                    uploadExportFilesToS3(transferManager.get(), outputDirectory, outputS3ObjectInfo);
                    uploadGcLogToS3(transferManager.get(), outputDirectory, outputS3ObjectInfo);
                });

                logger.warn("Failed export S3 location: {}", outputS3ObjectInfo.toString());
            }
        } catch (Exception e) {
            logger.error("Failed to upload failed export files to S3", e);
        }
    }

    private void uploadGcLogToS3(TransferManager transferManager,
                                 File directory,
                                 S3ObjectInfo outputS3ObjectInfo) throws IOException {


        File gcLog = new File(directory, "./../gc.log");

        if (!gcLog.exists()) {
            logger.warn("Ignoring request to upload GC log to S3 because GC log does not exist");
            return;
        }

        S3ObjectInfo gcLogS3ObjectInfo = outputS3ObjectInfo.withNewKeySuffix("gc.log");

        try (InputStream inputStream = new FileInputStream(gcLog)) {

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(gcLog.length());
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

            PutObjectRequest putObjectRequest = new PutObjectRequest(gcLogS3ObjectInfo.bucket(),
                    gcLogS3ObjectInfo.key(),
                    inputStream,
                    objectMetadata).withTagging(createObjectTags(profiles));

            Upload upload = transferManager.upload(putObjectRequest);

            upload.waitForUploadResult();

        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private S3ObjectInfo calculateOutputS3Path(File outputDirectory) {
        S3ObjectInfo outputBaseS3ObjectInfo = new S3ObjectInfo(outputS3Path);

        if (s3UploadParams.createExportSubdirectory()) {
            return outputBaseS3ObjectInfo.withNewKeySuffix(outputDirectory.getName());
        } else {
            return outputBaseS3ObjectInfo;
        }
    }

    private void uploadCompletionFileToS3(TransferManager transferManager,
                                          File directory,
                                          S3ObjectInfo outputS3ObjectInfo,
                                          ExportStats stats,
                                          GraphSchema graphSchema) throws IOException {

        if (StringUtils.isEmpty(completionFileS3Path)) {
            return;
        }

        if (directory == null || !directory.exists()) {
            logger.warn("Ignoring request to upload completion file to S3 because directory from which to upload files does not exist");
            return;
        }

        String completionFilename = s3UploadParams.createExportSubdirectory() ?
                directory.getName() :
                String.valueOf(System.currentTimeMillis());
        File completionFile = new File(localOutputPath, completionFilename + ".json");

        ObjectNode neptuneExportNode = JsonNodeFactory.instance.objectNode();
        completionFilePayload.set("neptuneExport", neptuneExportNode);
        neptuneExportNode.put("outputS3Path", outputS3ObjectInfo.toString());
        stats.addTo(neptuneExportNode, graphSchema);

        for (CompletionFileWriter completionFileWriter : completionFileWriters) {
            completionFileWriter.updateCompletionFile(completionFilePayload);
        }

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(completionFile), UTF_8))) {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            writer.write(objectWriter.writeValueAsString(completionFilePayload));
        }

        S3ObjectInfo completionFileS3ObjectInfo =
                new S3ObjectInfo(completionFileS3Path).replaceOrAppendKey(
                        "_COMPLETION_ID_",
                        FilenameUtils.getBaseName(completionFile.getName()),
                        completionFile.getName());


        try (InputStream inputStream = new FileInputStream(completionFile)) {

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(completionFile.length());
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

            PutObjectRequest putObjectRequest = new PutObjectRequest(completionFileS3ObjectInfo.bucket(),
                    completionFileS3ObjectInfo.key(),
                    inputStream,
                    objectMetadata).withTagging(createObjectTags(profiles));

            Upload upload = transferManager.upload(putObjectRequest);

            upload.waitForUploadResult();

        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void uploadExportFilesToS3(TransferManager transferManager, File directory, S3ObjectInfo outputS3ObjectInfo) {

        if (directory == null || !directory.exists()) {
            logger.warn("Ignoring request to upload files to S3 because upload directory from which to upload files does not exist");
            return;
        }

        boolean allowRetry = true;
        int retryCount = 0;

        while (allowRetry){
            try {

                //deleteS3Directories(directory, outputS3ObjectInfo);

                ObjectMetadataProvider metadataProvider = (file, objectMetadata) -> {
                    objectMetadata.setContentLength(file.length());
                    objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                };

                ObjectTaggingProvider taggingProvider = uploadContext -> createObjectTags(profiles);

                logger.info("Uploading export files to {}", outputS3ObjectInfo.key());

                MultipleFileUpload upload = transferManager.uploadDirectory(
                        outputS3ObjectInfo.bucket(),
                        outputS3ObjectInfo.key(),
                        directory,
                        true,
                        metadataProvider,
                        taggingProvider);

                AmazonClientException amazonClientException = upload.waitForException();

                if (amazonClientException != null){
                    String errorMessage = amazonClientException.getMessage();
                    logger.error("Upload to S3 failed: {}", errorMessage);
                    if (!amazonClientException.isRetryable() || retryCount > 2){
                        allowRetry = false;
                        logger.warn("Cancelling upload to S3 [RetryCount: {}]", retryCount);
                        throw new RuntimeException(String.format("Upload to S3 failed [Directory: %s, S3 location: %s, Reason: %s, RetryCount: %s]", directory, outputS3ObjectInfo, errorMessage, retryCount));
                    } else {
                        retryCount++;
                        logger.info("Retrying upload to S3 [RetryCount: {}]", retryCount);
                    }
                } else {
                    allowRetry = false;
                }
            } catch (InterruptedException e) {
                logger.warn(e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteS3Directories(Directories directories, S3ObjectInfo outputS3ObjectInfo) {

        if (!s3UploadParams.overwriteExisting()) {
            return;
        }

        List<S3ObjectInfo> leafS3Directories = new ArrayList<>();

        Path rootDirectory = directories.rootDirectory();
        for (Path subdirectory : directories.subdirectories()) {
            String newKey = rootDirectory.relativize(subdirectory).toString();
            leafS3Directories.add(outputS3ObjectInfo.withNewKeySuffix(newKey));
        }
    }

}
