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

import com.amazonaws.services.neptune.dgl.DglNeptuneExportEventHandler;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.neptune.util.TransferManagerWrapper;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NeptuneExportService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeptuneExportService.class);

    public static final List<Tag> TAGS = Collections.singletonList(new Tag("application", "neptune-export"));

    private final String cmd;
    private final String localOutputPath;
    private final String outputS3Path;
    private final String configFileS3Path;
    private final String queriesFileS3Path;
    private final String completionFileS3Path;
    private final ObjectNode completionFilePayload;
    private final int maxConcurrency;

    public NeptuneExportService(String cmd,
                                String localOutputPath,
                                String outputS3Path,
                                String configFileS3Path,
                                String queriesFileS3Path,
                                String completionFileS3Path,
                                ObjectNode completionFilePayload,
                                int maxConcurrency) {
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

        try (TransferManagerWrapper transferManager = new TransferManagerWrapper()) {

            clearTempFiles();

            if (StringUtils.isNotEmpty(configFileS3Path)) {
                updateArgs(args, "--config-file", downloadFile(transferManager.get(), configFileS3Path));
            }
            if (StringUtils.isNotEmpty(queriesFileS3Path)) {
                updateArgs(args, "--queries", downloadFile(transferManager.get(), queriesFileS3Path));
            }
        }

        ExportToS3NeptuneExportEventHandler eventHandler = new ExportToS3NeptuneExportEventHandler(
                localOutputPath,
                outputS3Path,
                completionFileS3Path,
                completionFilePayload);

        DglNeptuneExportEventHandler dglEventHandler = new DglNeptuneExportEventHandler(localOutputPath, outputS3Path);

        EventHandlerCollection eventHandlerCollection = new EventHandlerCollection(
                Arrays.asList(eventHandler, dglEventHandler));

        new NeptuneExportRunner(args.values(), eventHandlerCollection).run();

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
