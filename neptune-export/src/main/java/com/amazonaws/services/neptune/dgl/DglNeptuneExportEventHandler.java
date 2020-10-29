/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.export.NeptuneExportEventHandler;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.neptune.util.TransferManagerWrapper;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.function.Function;

import static com.amazonaws.services.neptune.export.NeptuneExportService.TAGS;

public class DglNeptuneExportEventHandler implements NeptuneExportEventHandler {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DglNeptuneExportEventHandler.class);

    private static final String FILE_NAME = "training-job-configuration.json";

    private final String localOutputPath;
    private final String outputS3Path;
    private final Args args;

    public DglNeptuneExportEventHandler(String localOutputPath, String outputS3Path, Args args) {
        this.localOutputPath = localOutputPath;
        this.outputS3Path = outputS3Path;
        this.args = args;
    }

    @Override
    public void onExportComplete(Path outputPath, ExportStats stats) throws Exception {
        //Do nothing
    }

    @Override
    public void onExportComplete(Path outputPath, ExportStats stats, GraphSchema graphSchema) throws Exception {

        Function<PropertySchema, String> getColumnName = args.contains("--exclude-type-definitions") ?
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE :
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITH_DATATYPE;

        try (TransferManagerWrapper transferManager = new TransferManagerWrapper()) {
            File outputDirectory = outputPath.toFile();
            S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory);

            File trainingJobConfigurationFile = new File(new File(localOutputPath), FILE_NAME);

            try (Writer writer = new PrintWriter(trainingJobConfigurationFile)) {
                new JobTrainingConfigurationFileWriter(graphSchema, createJsonGenerator(writer), getColumnName).write();
            }

            Timer.timedActivity("uploading training job configuration file to S3", (CheckedActivity.Runnable) () -> {
                uploadTrainingJobConfigurationFileToS3(
                        transferManager.get(),
                        trainingJobConfigurationFile,
                        outputS3ObjectInfo);
            });
        }
    }

    private void uploadTrainingJobConfigurationFileToS3(TransferManager transferManager,
                                                        File trainingJobConfigurationFile,
                                                        S3ObjectInfo outputS3ObjectInfo) throws IOException {

        S3ObjectInfo s3ObjectInfo = outputS3ObjectInfo.withNewKeySuffix(FILE_NAME);

        logger.info(FileUtils.readFileToString(trainingJobConfigurationFile, StandardCharsets.UTF_8));

        try (InputStream inputStream = new FileInputStream(trainingJobConfigurationFile)) {

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(trainingJobConfigurationFile.length());
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

            PutObjectRequest putObjectRequest = new PutObjectRequest(s3ObjectInfo.bucket(),
                    s3ObjectInfo.key(),
                    inputStream,
                    objectMetadata).withTagging(new ObjectTagging(TAGS));

            Upload upload = transferManager.upload(putObjectRequest);

            upload.waitForUploadResult();

        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private S3ObjectInfo calculateOutputS3Path(File outputDirectory) {
        S3ObjectInfo outputBaseS3ObjectInfo = new S3ObjectInfo(outputS3Path);
        return outputBaseS3ObjectInfo.withNewKeySuffix(outputDirectory.getName());
    }

    private JsonGenerator createJsonGenerator(Writer writer) throws IOException {
        JsonGenerator generator = new JsonFactory().createGenerator(writer);
        generator.setPrettyPrinter(new DefaultPrettyPrinter());
        return generator;
    }
}
