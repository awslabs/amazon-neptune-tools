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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.neptune.util.EnvironmentVariableUtils;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneExportLambda implements RequestStreamHandler {

    private static final String TEMP_PATH = "/tmp/neptune";

    private final String localOutputPath;

    public NeptuneExportLambda(){
        this(TEMP_PATH);
    }

    public NeptuneExportLambda(String localOutputPath) {
        this.localOutputPath = localOutputPath;
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        Logger logger = s -> context.getLogger().log(s);

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

        String queriesFileS3Path = json.has("queriesFileS3Path") ?
                json.path("queriesFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("QUERIES_FILE_S3_PATH", "");

        String completionFileS3Path = json.has("completionFileS3Path") ?
                json.path("completionFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("COMPLETION_FILE_S3_PATH", "");

        ObjectNode completionFilePayload = json.has("completionFilePayload") ?
                json.path("completionFilePayload").deepCopy() :
                new ObjectMapper().readTree(
                        EnvironmentVariableUtils.getOptionalEnv(
                                "COMPLETION_FILE_PAYLOAD",
                                "{}")).
                        deepCopy();

        int maxConcurrency = json.has("jobSize") ?
                JobSize.parse(json.path("jobSize").textValue()).maxConcurrency() :
                -1;

        logger.log("cmd                   : " + cmd);
        logger.log("outputS3Path          : " + outputS3Path);
        logger.log("configFileS3Path      : " + configFileS3Path);
        logger.log("queriesFileS3Path     : " + queriesFileS3Path);
        logger.log("completionFileS3Path  : " + completionFileS3Path);
        logger.log("completionFilePayload : " + completionFilePayload.toString());

        NeptuneExportService neptuneExportService = new NeptuneExportService(
                logger,
                cmd,
                localOutputPath,
                outputS3Path,
                configFileS3Path,
                queriesFileS3Path,
                completionFileS3Path,
                completionFilePayload,
                maxConcurrency);

        S3ObjectInfo outputS3ObjectInfo = neptuneExportService.execute();

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream, UTF_8))) {
            writer.write(outputS3ObjectInfo.toString());
        }
    }
}
