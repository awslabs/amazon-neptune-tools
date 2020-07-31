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

import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.s3.transfer.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneExportService {

    private final Logger logger;
    private final String cmd;
    private final String localOutputPath;
    private final String outputS3Path;
    private final String configFileS3Path;
    private final String queriesFileS3Path;
    private final String completionFileS3Path;
    private final ObjectNode completionFilePayload;
    private final int maxConcurrency;

    public NeptuneExportService(Logger logger,
                                String cmd,
                                String localOutputPath,
                                String outputS3Path,
                                String configFileS3Path,
                                String queriesFileS3Path,
                                String completionFileS3Path,
                                ObjectNode completionFilePayload,
                                int maxConcurrency) {
        this.logger = logger;
        this.cmd = cmd;
        this.localOutputPath = localOutputPath;
        this.outputS3Path = outputS3Path;
        this.configFileS3Path = configFileS3Path;
        this.queriesFileS3Path = queriesFileS3Path;
        this.completionFileS3Path = completionFileS3Path;
        this.completionFilePayload = completionFilePayload;
        this.maxConcurrency = maxConcurrency;
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
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TransferManager transferManager = TransferManagerBuilder.standard().build();

        clearTempFiles();

        if (StringUtils.isNotEmpty(configFileS3Path)) {
            updateArgs(args, "--config-file", downloadFile(transferManager, configFileS3Path));
        }
        if (StringUtils.isNotEmpty(queriesFileS3Path)) {
            updateArgs(args, "--queries", downloadFile(transferManager, queriesFileS3Path));
        }

        ExportToS3NeptuneExportEventHandler eventHandler = new ExportToS3NeptuneExportEventHandler(
                logger,
                transferManager,
                localOutputPath,
                outputS3Path,
                completionFileS3Path,
                completionFilePayload);

        new NeptuneExportRunner(args.values(), eventHandler).run();

        transferManager.shutdownNow();

        return eventHandler.result();
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

        logger.log("Bucket: " + configFileS3ObjectInfo.bucket());
        logger.log("Key   : " + configFileS3ObjectInfo.key());
        logger.log("File  : " + file);

        Download download = transferManager.download(
                configFileS3ObjectInfo.bucket(),
                configFileS3ObjectInfo.key(),
                file);
        try {
            download.waitForCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(e.getMessage());
        }

        return file.getAbsoluteFile();
    }

    private static class ExportToS3NeptuneExportEventHandler implements NeptuneExportEventHandler {

        private final Logger logger;
        private final TransferManager transferManager;
        private final String localOutputPath;
        private final String outputS3Path;
        private final String completionFileS3Path;
        private final ObjectNode completionFilePayload;
        private final AtomicReference<S3ObjectInfo> result = new AtomicReference<>();

        private ExportToS3NeptuneExportEventHandler(Logger logger,
                                                    TransferManager transferManager,
                                                    String localOutputPath,
                                                    String outputS3Path,
                                                    String completionFileS3Path,
                                                    ObjectNode completionFilePayload) {
            this.logger = logger;
            this.transferManager = transferManager;
            this.localOutputPath = localOutputPath;
            this.outputS3Path = outputS3Path;
            this.completionFileS3Path = completionFileS3Path;
            this.completionFilePayload = completionFilePayload;
        }

        @Override
        public void onExportComplete(Path outputPath, ExportStats stats) {
            File outputDirectory = outputPath.toFile();
            S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory);

            try (Timer timer = new Timer("uploading files to S3")) {
                uploadExportFilesToS3(transferManager, outputDirectory, outputS3ObjectInfo);
                uploadCompletionFileToS3(transferManager, outputDirectory, outputS3ObjectInfo, stats);
            }

            result.set(outputS3ObjectInfo);
        }

        public S3ObjectInfo result() {
            return result.get();
        }

        private S3ObjectInfo calculateOutputS3Path(File outputDirectory) {
            S3ObjectInfo outputBaseS3ObjectInfo = new S3ObjectInfo(outputS3Path);
            return outputBaseS3ObjectInfo.withNewKeySuffix(outputDirectory.getName());
        }

        private void uploadCompletionFileToS3(TransferManager transferManager,
                                              File directory,
                                              S3ObjectInfo outputS3ObjectInfo,
                                              ExportStats stats) {

            if (StringUtils.isEmpty(completionFileS3Path)) {
                return;
            }

            if (directory == null || !directory.exists()) {
                logger.log("Ignoring request to upload completion file to S3 because directory from which to upload files does not exist");
                return;
            }

            File completionFile = new File(localOutputPath, directory.getName() + ".json");

            ObjectNode neptuneExportNode = JsonNodeFactory.instance.objectNode();
            completionFilePayload.set("neptuneExport", neptuneExportNode);
            neptuneExportNode.put("outputS3Path", outputS3ObjectInfo.toString());
            stats.addTo(neptuneExportNode);

            try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(completionFile), UTF_8))) {
                ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
                writer.write(objectWriter.writeValueAsString(completionFilePayload));
            } catch (IOException e) {
                throw new RuntimeException("Error while writing completion file payload", e);
            }

            S3ObjectInfo completionFileS3ObjectInfo = new S3ObjectInfo(completionFileS3Path).withNewKeySuffix(completionFile.getName());

            Upload upload = transferManager.upload(completionFileS3ObjectInfo.bucket(), completionFileS3ObjectInfo.key(), completionFile);
            try {
                upload.waitForUploadResult();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(e.getMessage());
            }

        }

        private void uploadExportFilesToS3(TransferManager transferManager, File directory, S3ObjectInfo outputS3ObjectInfo) {

            if (directory == null || !directory.exists()) {
                logger.log("Ignoring request to upload files to S3 because upload directory from which to upload files does not exist");
                return;
            }

            try {

                MultipleFileUpload upload = transferManager.uploadDirectory(
                        outputS3ObjectInfo.bucket(),
                        outputS3ObjectInfo.key(),
                        directory,
                        true);

                upload.waitForCompletion();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(e.getMessage());
            }
        }
    }

}
