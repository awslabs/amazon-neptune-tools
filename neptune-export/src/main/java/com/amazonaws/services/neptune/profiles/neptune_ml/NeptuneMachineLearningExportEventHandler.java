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

package com.amazonaws.services.neptune.profiles.neptune_ml;

import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.export.NeptuneExportServiceEventHandler;
import com.amazonaws.services.neptune.propertygraph.EdgeLabelStrategy;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.function.Function;

import static com.amazonaws.services.neptune.export.NeptuneExportService.TAGS;

public class NeptuneMachineLearningExportEventHandler implements NeptuneExportServiceEventHandler {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeptuneMachineLearningExportEventHandler.class);

    private static final String FILE_NAME = "training-job-configuration.json";

    private final String outputS3Path;
    private final Args args;
    private final TrainingJobWriterConfig trainingJobWriterConfig;

    public NeptuneMachineLearningExportEventHandler(String outputS3Path,
                                                    ObjectNode additionalParams,
                                                    Args args) {

        logger.info("Adding neptune_ml event handler");

        this.outputS3Path = outputS3Path;
        this.args = args;
        this.trainingJobWriterConfig = createTrainingJobConfig(additionalParams);
    }

    private TrainingJobWriterConfig createTrainingJobConfig(ObjectNode additionalParams) {
        JsonNode neptuneMlNode = additionalParams.path("neptune_ml");
        if (neptuneMlNode.isMissingNode()) {
            logger.info("No 'neptune_ml' config node in additional params so creating default training config");
            return new TrainingJobWriterConfig();
        } else {
            TrainingJobWriterConfig trainingJobWriterConfig = TrainingJobWriterConfig.fromJson(neptuneMlNode);
            logger.info("Training job writer config: {}", trainingJobWriterConfig);
            return trainingJobWriterConfig;
        }
    }

    @Override
    public void onBeforeExport(Args args) {
        if (!args.contains("--merge-files")) {
            args.addFlag("--merge-files");
        }

        if (!args.contains("--edge-label-strategy")) {
            args.addOption("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name());
        }

        if (args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeLabelsOnly.name())) {
            args.removeOptions("--edge-label-strategy");
            args.addOption("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name());
        }
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

            File trainingJobConfigurationFile = new File(outputPath.toFile(), FILE_NAME);

            try (Writer writer = new PrintWriter(trainingJobConfigurationFile)) {
                new JobTrainingConfigurationFileWriter(
                        graphSchema,
                        createJsonGenerator(writer),
                        getColumnName,
                        trainingJobWriterConfig).write();
            }

            if (StringUtils.isNotEmpty(outputS3Path)) {
                Timer.timedActivity("uploading training job configuration file to S3",
                        (CheckedActivity.Runnable) () -> {
                            S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory);
                            uploadTrainingJobConfigurationFileToS3(
                                    transferManager.get(),
                                    trainingJobConfigurationFile,
                                    outputS3ObjectInfo);
                        });
            }
        }
    }

    private void uploadTrainingJobConfigurationFileToS3(TransferManager transferManager,
                                                        File trainingJobConfigurationFile,
                                                        S3ObjectInfo outputS3ObjectInfo) throws IOException {

        S3ObjectInfo s3ObjectInfo = outputS3ObjectInfo.withNewKeySuffix(FILE_NAME);

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
