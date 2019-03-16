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

package com.amazonaws.services.neptune.service;

import com.amazonaws.services.neptune.NeptuneExportCli;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.s3.transfer.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;

import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneExportService {

    private final String TEMP_PATH = "/tmp/neptune";

    private final Logger logger;
    private final String cmd;
    private final String outputS3Path;
    private final String configFileS3Path;
    private final String queriesFileS3Path;
    private final String completionFileS3Path;
    private final ObjectNode completionFilePayload;

    public NeptuneExportService(Logger logger,
                                String cmd,
                                String outputS3Path,
                                String configFileS3Path,
                                String queriesFileS3Path,
                                String completionFileS3Path,
                                ObjectNode completionFilePayload) {
        this.logger = logger;
        this.cmd = cmd;
        this.outputS3Path = outputS3Path;
        this.configFileS3Path = configFileS3Path;
        this.queriesFileS3Path = queriesFileS3Path;
        this.completionFileS3Path = completionFileS3Path;
        this.completionFilePayload = completionFilePayload;
    }

    public S3ObjectInfo execute() throws IOException {

        Args args;
        try {
            args = new Args(cmd);
            args.removeOptions("-c", "--config-file");
            args.removeOptions("--queries");
            args.removeOptions("-d", "--dir");
            args.addOption("-d", new File(TEMP_PATH, "output").getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TransferManager transferManager = TransferManagerBuilder.standard().build();

        clearTempFiles();
        downloadFile(transferManager, configFileS3Path, args, "--config-file");
        downloadFile(transferManager, queriesFileS3Path, args, "--queries");

        File outputDirectory = executeCommand(args);

        S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory);

        uploadExportFilesToS3(transferManager, outputDirectory, outputS3ObjectInfo);
        uploadCompletionFileToS3(transferManager, outputDirectory, outputS3ObjectInfo);

        return outputS3ObjectInfo;
    }

    private S3ObjectInfo calculateOutputS3Path(File outputDirectory) {
        S3ObjectInfo outputBaseS3ObjectInfo = new S3ObjectInfo(outputS3Path);
        return outputBaseS3ObjectInfo.withNewKeySuffix(outputDirectory.getName());
    }

    private void clearTempFiles() throws IOException {
        File directory = new File(TEMP_PATH);
        if (directory.exists() && directory.isDirectory()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    private void uploadCompletionFileToS3(TransferManager transferManager, File directory, S3ObjectInfo outputS3ObjectInfo) throws IOException {
        if (!completionFileS3Path.isEmpty()) {

            File completionFile = new File(TEMP_PATH, directory.getName() + ".json");

            ObjectNode neptuneExportNode = JsonNodeFactory.instance.objectNode();
            neptuneExportNode.put("output", outputS3ObjectInfo.toString());
            completionFilePayload.set("neptuneExport", neptuneExportNode);

            try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(completionFile), UTF_8))) {
                ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
                writer.write(objectWriter.writeValueAsString(completionFilePayload));
            }

            S3ObjectInfo completionFileS3ObjectInfo = new S3ObjectInfo(completionFileS3Path).withNewKeySuffix(completionFile.getName());

            Upload upload = transferManager.upload(completionFileS3ObjectInfo.bucket(), completionFileS3ObjectInfo.key(), completionFile);
            try {
                upload.waitForUploadResult();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void uploadExportFilesToS3(TransferManager transferManager, File directory, S3ObjectInfo outputS3ObjectInfo) {
        try {

            MultipleFileUpload upload = transferManager.uploadDirectory(
                    outputS3ObjectInfo.bucket(),
                    outputS3ObjectInfo.key(),
                    directory,
                    true);

            upload.waitForCompletion();
        } catch (InterruptedException e) {
            logger.log(e.getMessage());
        }
    }

    private void downloadFile(TransferManager transferManager, String s3Path, Args args, String option) {

        if (!s3Path.isEmpty()) {

            S3ObjectInfo configFileS3ObjectInfo = new S3ObjectInfo(s3Path);
            File file = configFileS3ObjectInfo.createDownloadFile(TEMP_PATH);

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
                logger.log(e.getMessage());
            }

            args.addOption(option, file.getAbsolutePath());
        }
    }

    private File executeCommand(Args args) throws IOException {

        logger.log("ARGS: " + args.toString());

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {

            PrintStream out = new PrintStream(output);
            PrintStream old = System.out;
            System.setOut(out);

            NeptuneExportCli.main(args.values());

            System.out.flush();
            System.setOut(old);

            return new File(output.toString().replace("\n", ""));
        }
    }

}
