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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.amazonaws.services.neptune.util.GitProperties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.neptune.util.EnvironmentVariableUtils;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.amazonaws.services.neptune.RunNeptuneExportSvc.DEFAULT_MAX_FILE_DESCRIPTOR_COUNT;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneExportLambda implements RequestStreamHandler {

    public static final String TEMP_PATH = "/tmp/neptune";

    private final String localOutputPath;
    private final boolean cleanOutputPath;
    private final int maxFileDescriptorCount;

    public NeptuneExportLambda() {
        this(TEMP_PATH, true, DEFAULT_MAX_FILE_DESCRIPTOR_COUNT);
    }

    public NeptuneExportLambda(String localOutputPath, boolean cleanOutputPath, int maxFileDescriptorCount) {
        this.localOutputPath = localOutputPath;
        this.cleanOutputPath = cleanOutputPath;
        this.maxFileDescriptorCount = maxFileDescriptorCount;
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        Logger logger = s -> context.getLogger().log(s);

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode json = objectMapper.readTree(IOUtils.toString(inputStream, UTF_8.name()));

        String cmd = json.has("command") ?
                json.path("command").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("COMMAND", "export-pg");

        ObjectNode params = json.has("params") ?
                (ObjectNode) json.get("params") :
                objectMapper.readTree("{}").deepCopy();

        String outputS3Path = json.has("outputS3Path") ?
                json.path("outputS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("OUTPUT_S3_PATH", "");

        boolean createExportSubdirectory = Boolean.parseBoolean(
                json.has("createExportSubdirectory") ?
                        json.path("createExportSubdirectory").toString() :
                        EnvironmentVariableUtils.getOptionalEnv("CREATE_EXPORT_SUBDIRECTORY", "true"));

        boolean overwriteExisting = Boolean.parseBoolean(
                json.has("overwriteExisting") ?
                        json.path("overwriteExisting").toString() :
                        EnvironmentVariableUtils.getOptionalEnv("OVERWRITE_EXISTING", "false"));

        boolean uploadToS3OnError = Boolean.parseBoolean(
                json.has("uploadToS3OnError") ?
                        json.path("uploadToS3OnError").toString() :
                        EnvironmentVariableUtils.getOptionalEnv("UPLOAD_TO_S3_ON_ERROR", "true"));

        String configFileS3Path = json.has("configFileS3Path") ?
                json.path("configFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("CONFIG_FILE_S3_PATH", "");

        String queriesFileS3Path = json.has("queriesFileS3Path") ?
                json.path("queriesFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("QUERIES_FILE_S3_PATH", "");

        String completionFileS3Path = json.has("completionFileS3Path") ?
                json.path("completionFileS3Path").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("COMPLETION_FILE_S3_PATH", "");

        String s3Region = json.has("s3Region") ?
                json.path("s3Region").textValue() :
                EnvironmentVariableUtils.getOptionalEnv("S3_REGION",
                        EnvironmentVariableUtils.getOptionalEnv("AWS_REGION", ""));

        ObjectNode completionFilePayload = json.has("completionFilePayload") ?
                json.path("completionFilePayload").deepCopy() :
                objectMapper.readTree(
                        EnvironmentVariableUtils.getOptionalEnv(
                                "COMPLETION_FILE_PAYLOAD",
                                "{}")).
                        deepCopy();

        ObjectNode additionalParams = json.has("additionalParams") ?
                json.path("additionalParams").deepCopy() :
                objectMapper.readTree("{}").deepCopy();

        int maxConcurrency = json.has("jobSize") ?
                JobSize.parse(json.path("jobSize").textValue()).maxConcurrency() :
                -1;

        logger.log("cmd                       : " + cmd);
        logger.log("params                    : " + params.toPrettyString());
        logger.log("outputS3Path              : " + outputS3Path);
        logger.log("createExportSubdirectory  : " + createExportSubdirectory);
        logger.log("overwriteExisting         : " + overwriteExisting);
        logger.log("uploadToS3OnError         : " + uploadToS3OnError);
        logger.log("configFileS3Path          : " + configFileS3Path);
        logger.log("queriesFileS3Path         : " + queriesFileS3Path);
        logger.log("completionFileS3Path      : " + completionFileS3Path);
        logger.log("s3Region                  : " + s3Region);
        logger.log("completionFilePayload     : " + completionFilePayload.toPrettyString());
        logger.log("additionalParams          : " + additionalParams.toPrettyString());

        if (!cmd.contains(" ") && !params.isEmpty()) {
            cmd = ParamConverter.fromJson(cmd, params).toString();
        }

        logger.log("revised cmd           : " + cmd);

        NeptuneExportService neptuneExportService = new NeptuneExportService(
                cmd,
                localOutputPath,
                cleanOutputPath,
                outputS3Path,
                createExportSubdirectory,
                overwriteExisting,
                uploadToS3OnError,
                configFileS3Path,
                queriesFileS3Path,
                completionFileS3Path,
                completionFilePayload,
                additionalParams,
                maxConcurrency,
                s3Region,
                maxFileDescriptorCount);

        S3ObjectInfo outputS3ObjectInfo = neptuneExportService.execute();

        if (StringUtils.isEmpty(outputS3Path)) {
            return;
        }

        if (outputS3ObjectInfo != null) {
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream, UTF_8))) {
                writer.write(outputS3ObjectInfo.toString());
            }
        } else {
            System.exit(-1);
        }
    }
}
