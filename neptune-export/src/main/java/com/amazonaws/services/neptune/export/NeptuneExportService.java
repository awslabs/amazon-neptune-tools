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

import com.amazonaws.services.neptune.profiles.incremental_export.IncrementalExportEventHandler;
import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMachineLearningExportEventHandlerV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMachineLearningExportEventHandlerV2;
import com.amazonaws.services.neptune.util.EnvironmentVariableUtils;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.neptune.util.TransferManagerWrapper;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class NeptuneExportService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeptuneExportService.class);

    public static final List<Tag> NEPTUNE_EXPORT_TAGS = Collections.singletonList(new Tag("application", "neptune-export"));
    public static final String NEPTUNE_ML_PROFILE_NAME = "neptune_ml";
    public static final String INCREMENTAL_EXPORT_PROFILE_NAME = "incremental_export";

    private final String cmd;
    private final String localOutputPath;
    private final boolean cleanOutputPath;
    private final String outputS3Path;
    private final boolean createExportSubdirectory;
    private final boolean overwriteExisting;
    private final boolean uploadToS3OnError;
    private final String configFileS3Path;
    private final String queriesFileS3Path;
    private final String completionFileS3Path;
    private final ObjectNode completionFilePayload;
    private final ObjectNode additionalParams;
    private final int maxConcurrency;
    private final String s3Region;

    public NeptuneExportService(String cmd,
                                String localOutputPath,
                                boolean cleanOutputPath,
                                String outputS3Path,
                                boolean createExportSubdirectory,
                                boolean overwriteExisting,
                                boolean uploadToS3OnError,
                                String configFileS3Path,
                                String queriesFileS3Path,
                                String completionFileS3Path,
                                ObjectNode completionFilePayload,
                                ObjectNode additionalParams,
                                int maxConcurrency,
                                String s3Region) {
        this.cmd = cmd;
        this.localOutputPath = localOutputPath;
        this.cleanOutputPath = cleanOutputPath;
        this.outputS3Path = outputS3Path;
        this.createExportSubdirectory = createExportSubdirectory;
        this.overwriteExisting = overwriteExisting;
        this.uploadToS3OnError = uploadToS3OnError;
        this.configFileS3Path = configFileS3Path;
        this.queriesFileS3Path = queriesFileS3Path;
        this.completionFileS3Path = completionFileS3Path;
        this.completionFilePayload = completionFilePayload;
        this.additionalParams = additionalParams;
        this.maxConcurrency = maxConcurrency;
        this.s3Region = s3Region;
    }

    public S3ObjectInfo execute() throws IOException {

        Args args;
        try {
            args = new Args(cmd);

            if (StringUtils.isNotEmpty(configFileS3Path)) {
                args.removeOptions("-c", "--config-file");
            }
            if (StringUtils.isNotEmpty(queriesFileS3Path)) {
                args.removeOptions("--queries");
            }

            if (args.contains("create-pg-config") ||
                    args.contains("export-pg") ||
                    args.contains("export-pg-from-config") ||
                    args.contains("export-pg-from-queries") ||
                    args.contains("export-rdf")) {

                args.removeOptions("-d", "--dir");
                args.addOption("-d", new File(localOutputPath, "output").getAbsolutePath());

                if (maxConcurrency > 0 && !args.contains("--clone-cluster-max-concurrency")) {
                    args.addOption("--clone-cluster-max-concurrency", String.valueOf(maxConcurrency));
                }

                if (!args.contains("--clone-cluster-correlation-id")){
                    String correlationId = EnvironmentVariableUtils.getOptionalEnv("AWS_BATCH_JOB_ID", null);
                    if (StringUtils.isNotEmpty(correlationId)){
                        args.addOption("--clone-cluster-correlation-id", correlationId);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (TransferManagerWrapper transferManager = new TransferManagerWrapper(s3Region)) {

            if (cleanOutputPath) {
                clearTempFiles();
            }

            if (StringUtils.isNotEmpty(configFileS3Path)) {
                updateArgs(args, "--config-file", downloadFile(transferManager.get(), configFileS3Path));
            }
            if (StringUtils.isNotEmpty(queriesFileS3Path)) {
                updateArgs(args, "--queries", downloadFile(transferManager.get(), queriesFileS3Path));
            }
        }

        if (additionalParams.has(NEPTUNE_ML_PROFILE_NAME) && (!args.contains("--profile", NEPTUNE_ML_PROFILE_NAME))) {
            args.addOption("--profile", NEPTUNE_ML_PROFILE_NAME);
        }

        Collection<String> profiles = args.getOptionValues("--profile");

        if (!createExportSubdirectory && !overwriteExisting) {
            checkS3OutputIsEmpty();
        }

        EventHandlerCollection eventHandlerCollection = new EventHandlerCollection();

        Collection<CompletionFileWriter> completionFileWriters = new ArrayList<>();
        ExportToS3NeptuneExportEventHandler.S3UploadParams s3UploadParams =
                new ExportToS3NeptuneExportEventHandler.S3UploadParams()
                .setCreateExportSubdirectory(createExportSubdirectory)
                .setOverwriteExisting(overwriteExisting);

        ExportToS3NeptuneExportEventHandler exportToS3EventHandler = new ExportToS3NeptuneExportEventHandler(
                localOutputPath,
                outputS3Path,
                s3Region,
                completionFileS3Path,
                completionFilePayload,
                uploadToS3OnError,
                s3UploadParams,
                profiles,
                completionFileWriters);

        eventHandlerCollection.addHandler(exportToS3EventHandler);

        if (profiles.contains(NEPTUNE_ML_PROFILE_NAME)) {

            JsonNode neptuneMlNode = additionalParams.path(NEPTUNE_ML_PROFILE_NAME);

            boolean useV2 =  args.contains("--feature-toggle", FeatureToggle.NeptuneML_V2.name()) ||
                    (neptuneMlNode.has("version") && neptuneMlNode.get("version").textValue().startsWith("v2."));

            boolean useV1 =  (neptuneMlNode.has("version") && neptuneMlNode.get("version").textValue().startsWith("v1."));

            if (useV1) {
                NeptuneMachineLearningExportEventHandlerV1 neptuneMlEventHandler =
                        new NeptuneMachineLearningExportEventHandlerV1(
                                outputS3Path,
                                s3Region,
                                createExportSubdirectory,
                                additionalParams,
                                args,
                                profiles);
                eventHandlerCollection.addHandler(neptuneMlEventHandler);
            } else {
                NeptuneMachineLearningExportEventHandlerV2 neptuneMlEventHandler =
                        new NeptuneMachineLearningExportEventHandlerV2(
                                outputS3Path,
                                s3Region,
                                createExportSubdirectory,
                                additionalParams,
                                args,
                                profiles);
                eventHandlerCollection.addHandler(neptuneMlEventHandler);
            }
        }

        if (profiles.contains(INCREMENTAL_EXPORT_PROFILE_NAME)) {

            IncrementalExportEventHandler incrementalExportEventHandler = new IncrementalExportEventHandler(additionalParams);
            completionFileWriters.add(incrementalExportEventHandler);
            eventHandlerCollection.addHandler(incrementalExportEventHandler);
        }


        eventHandlerCollection.onBeforeExport(args, s3UploadParams);

        logger.info("Args after service init: {}", String.join(" ", args.values()));

        new NeptuneExportRunner(args.values(), eventHandlerCollection, false).run();

        return exportToS3EventHandler.result();
    }

    private void checkS3OutputIsEmpty() {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(outputS3Path);
        ObjectListing listing = s3.listObjects(
                new ListObjectsRequest(
                        s3ObjectInfo.bucket(),
                        s3ObjectInfo.key(),
                        null,
                        null,
                        1));
        if (!listing.getObjectSummaries().isEmpty()) {
            throw new IllegalStateException(String.format("S3 destination contains existing objects: %s. Set 'overwriteExisting' parameter to 'true' to allow overwriting existing objects.", outputS3Path));
        }
    }

    private void clearTempFiles() throws IOException {
        File directory = new File(localOutputPath);
        if (directory.exists() && directory.isDirectory()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    private void updateArgs(Args args, String option, Object value) {
        if (value != null) {
            args.addOption(option, value.toString());
        }
    }

    private File downloadFile(TransferManager transferManager, String s3Path) {

        if (StringUtils.isEmpty(s3Path)) {
            return null;
        }

        S3ObjectInfo configFileS3ObjectInfo = new S3ObjectInfo(s3Path);
        File file = configFileS3ObjectInfo.createDownloadFile(localOutputPath);

        logger.info("Bucket: " + configFileS3ObjectInfo.bucket());
        logger.info("Key   : " + configFileS3ObjectInfo.key());
        logger.info("File  : " + file);

        Download download = transferManager.download(
                configFileS3ObjectInfo.bucket(),
                configFileS3ObjectInfo.key(),
                file);
        try {
            download.waitForCompletion();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            Thread.currentThread().interrupt();
        }

        return file.getAbsoluteFile();
    }

}
