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
import com.amazonaws.services.neptune.export.ExportToS3NeptuneExportEventHandler;
import com.amazonaws.services.neptune.export.NeptuneExportServiceEventHandler;
import com.amazonaws.services.neptune.propertygraph.EdgeLabelStrategy;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.neptune.util.TransferManagerWrapper;
import com.amazonaws.services.s3.model.ObjectMetadata;
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
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.function.Function;

public class NeptuneMachineLearningExportEventHandler implements NeptuneExportServiceEventHandler {

    public static final String NEPTUNE_ML_PROFILE_NAME = "neptune_ml";

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeptuneMachineLearningExportEventHandler.class);

    private final String outputS3Path;
    private final Args args;
    private final Collection<TrainingJobWriterConfig> trainingJobWriterConfigCollection;
    private final Collection<String> profiles;
    private final boolean createExportSubdirectory;
    private final PrinterOptions printerOptions;

    public NeptuneMachineLearningExportEventHandler(String outputS3Path,
                                                    boolean createExportSubdirectory,
                                                    ObjectNode additionalParams,
                                                    Args args,
                                                    Collection<String> profiles) {
        logger.info("Adding neptune_ml event handler");

        this.outputS3Path = outputS3Path;
        this.createExportSubdirectory = createExportSubdirectory;
        this.args = args;
        this.trainingJobWriterConfigCollection = createTrainingJobConfigCollection(additionalParams);
        this.profiles = profiles;
        this.printerOptions = new PrinterOptions(false, args.contains("--escape-csv-headers"), true);
    }

    private Collection<TrainingJobWriterConfig> createTrainingJobConfigCollection(ObjectNode additionalParams) {
        JsonNode neptuneMlNode = additionalParams.path(NEPTUNE_ML_PROFILE_NAME);
        if (neptuneMlNode.isMissingNode()) {
            logger.info("No 'neptune_ml' config node in additional params so creating default training config");
            return Collections.singletonList(new TrainingJobWriterConfig());
        } else {
            Collection<TrainingJobWriterConfig> trainingJobWriterConfig = TrainingJobWriterConfig.fromJson(neptuneMlNode);
            logger.info("Training job writer config: {}", trainingJobWriterConfig);
            return trainingJobWriterConfig;
        }
    }

    @Override
    public void onBeforeExport(Args args) {
        if (args.contains("export-pg") && !args.contains("--exclude-type-definitions")) {
            args.addFlag("--exclude-type-definitions");
        }

        if (args.contains("export-pg") &&
                args.containsAny("--config", "--filter", "-c", "--config-file", "--filter-config-file")) {
            args.replace("export-pg", "export-pg-from-config");
        }

        if (!args.contains("--merge-files")) {
            args.addFlag("--merge-files");
        }

        if (args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeLabelsOnly.name())) {
            args.removeOptions("--edge-label-strategy");
        }

        if (!args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name())) {
            args.addOption("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name());
        }

        if (args.contains("--export-id")) {
            args.removeOptions("--export-id");
        }

        args.addOption("--export-id", new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
    }

    @Override
    public void onExportComplete(Path outputPath, ExportStats stats) throws Exception {
        //Do nothing
    }

    @Override
    public void onExportComplete(Path outputPath, ExportStats stats, GraphSchema graphSchema) throws Exception {

        PropertyName propertyName = args.contains("--exclude-type-definitions") ?
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE :
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITH_DATATYPE;

        try (TransferManagerWrapper transferManager = new TransferManagerWrapper()) {
            for (TrainingJobWriterConfig trainingJobWriterConfig : trainingJobWriterConfigCollection) {
                createTrainingJobConfigurationFile(trainingJobWriterConfig, outputPath, graphSchema, propertyName, transferManager);
            }
        }
    }

    private void createTrainingJobConfigurationFile(TrainingJobWriterConfig trainingJobWriterConfig,
                                                    Path outputPath,
                                                    GraphSchema graphSchema,
                                                    PropertyName propertyName,
                                                    TransferManagerWrapper transferManager) throws Exception {

        File outputDirectory = outputPath.toFile();
        String filename = String.format("%s.json", trainingJobWriterConfig.name());
        File trainingJobConfigurationFile = new File(outputPath.toFile(), filename);

        try (Writer writer = new PrintWriter(trainingJobConfigurationFile)) {
            new JobTrainingConfigurationFileWriter(
                    graphSchema,
                    createJsonGenerator(writer),
                    propertyName,
                    printerOptions,
                    trainingJobWriterConfig).write();
        }

        if (StringUtils.isNotEmpty(outputS3Path)) {
            Timer.timedActivity("uploading training job configuration file to S3",
                    (CheckedActivity.Runnable) () -> {
                        S3ObjectInfo outputS3ObjectInfo = calculateOutputS3Path(outputDirectory);
                        uploadTrainingJobConfigurationFileToS3(
                                filename,
                                transferManager.get(),
                                trainingJobConfigurationFile,
                                outputS3ObjectInfo);
                    });
        }
    }

    private void uploadTrainingJobConfigurationFileToS3(String filename,
                                                        TransferManager transferManager,
                                                        File trainingJobConfigurationFile,
                                                        S3ObjectInfo outputS3ObjectInfo) throws IOException {

        S3ObjectInfo s3ObjectInfo = outputS3ObjectInfo.withNewKeySuffix(filename);

        try (InputStream inputStream = new FileInputStream(trainingJobConfigurationFile)) {

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(trainingJobConfigurationFile.length());
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

            PutObjectRequest putObjectRequest = new PutObjectRequest(s3ObjectInfo.bucket(),
                    s3ObjectInfo.key(),
                    inputStream,
                    objectMetadata).withTagging(ExportToS3NeptuneExportEventHandler.createObjectTags(profiles));

            Upload upload = transferManager.upload(putObjectRequest);

            upload.waitForUploadResult();

        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private S3ObjectInfo calculateOutputS3Path(File outputDirectory) {
        S3ObjectInfo outputBaseS3ObjectInfo = new S3ObjectInfo(outputS3Path);
        if (createExportSubdirectory){
            return outputBaseS3ObjectInfo.withNewKeySuffix(outputDirectory.getName());
        } else {
            return outputBaseS3ObjectInfo;
        }
    }

    private JsonGenerator createJsonGenerator(Writer writer) throws IOException {
        JsonGenerator generator = new JsonFactory().createGenerator(writer);
        generator.setPrettyPrinter(new DefaultPrettyPrinter());
        return generator;
    }
}
