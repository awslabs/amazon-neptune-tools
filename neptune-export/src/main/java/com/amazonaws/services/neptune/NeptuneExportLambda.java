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

package com.amazonaws.services.neptune;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.neptune.util.EnvironmentVariableUtils;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.s3.transfer.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;

public class NeptuneExportLambda implements RequestStreamHandler {

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        LambdaLogger logger = context.getLogger();

        JsonNode json = new ObjectMapper().readTree(inputStream);

        String cmd = json.has("command") ?
                json.path("command").textValue() :
                EnvironmentVariableUtils.getMandatoryEnv("COMMAND");

        String outputS3Path = json.has("outputS3Path") ?
                json.path("outputS3Path").textValue() :
                EnvironmentVariableUtils.getMandatoryEnv("OUTPUT_S3_PATH");

        String configFileS3Path = json.has("configFileS3Path") ?
                json.path("configFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("CONFIG_FILE_S3_PATH", "");

        String completionFileS3Path = json.has("completionFileS3Path") ?
                json.path("completionFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("COMPLETION_FILE_S3_PATH", "");

        logger.log("cmd                  : " + cmd);
        logger.log("outputS3Path         : " + outputS3Path);
        logger.log("configFileS3Path     : " + configFileS3Path);
        logger.log("completionFileS3Path : " + completionFileS3Path);

        S3ObjectInfo outputBaseS3ObjectInfo = new S3ObjectInfo(outputS3Path);

        TransferManager transferManager = TransferManagerBuilder.standard().build();

        downloadConfigFile(context, logger, configFileS3Path, transferManager);

        File directory = executeCommand(cmd);
        logger.log("DIRECTORY: " + directory.getAbsolutePath());

        S3ObjectInfo outputS3ObjectInfo = outputBaseS3ObjectInfo.withNewKeySuffix(directory.getName());

        uploadExportFilesToS3(logger, transferManager, directory, outputS3ObjectInfo);
        uploadCompletionFileToS3(completionFileS3Path, transferManager, directory, outputS3ObjectInfo);

    }

    private void uploadCompletionFileToS3(String completionFileS3Path, TransferManager transferManager, File directory, S3ObjectInfo outputS3ObjectInfo) throws IOException {
        if (!completionFileS3Path.isEmpty()) {

            File completionFile = new File("/tmp", directory.getName() + ".txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(completionFile))) {
                writer.write(outputS3ObjectInfo.toString());
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

    private void uploadExportFilesToS3(LambdaLogger logger, TransferManager transferManager, File directory, S3ObjectInfo outputS3ObjectInfo) {
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

    private void downloadConfigFile(Context context, LambdaLogger logger, String configFileS3Path, TransferManager transferManager) {
        if (!configFileS3Path.isEmpty()) {
            S3ObjectInfo configFileS3ObjectInfo = new S3ObjectInfo(configFileS3Path);

            logger.log("Bucket: " + configFileS3ObjectInfo.bucket());
            logger.log("Key   : " + configFileS3ObjectInfo.key());
            logger.log("File  : " + configFileS3ObjectInfo.createDownloadFile("/tmp").getAbsolutePath());

            Download download = transferManager.download(
                    configFileS3ObjectInfo.bucket(),
                    configFileS3ObjectInfo.key(),
                    configFileS3ObjectInfo.createDownloadFile("/tmp"));
            try {
                download.waitForCompletion();
            } catch (InterruptedException e) {
                context.getLogger().log(e.getMessage());
            }
        }
    }

    private File executeCommand(String cmd) throws IOException {
        String[] args = cmd.split(" ");

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {

            PrintStream out = new PrintStream(output);
            PrintStream old = System.out;
            System.setOut(out);

            NeptuneExportCli.main(args);

            System.out.flush();
            System.setOut(old);

            return new File(output.toString().replace("\n", ""));
        }
    }
}
