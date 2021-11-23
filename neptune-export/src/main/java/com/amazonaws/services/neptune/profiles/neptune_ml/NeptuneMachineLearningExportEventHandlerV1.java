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

import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.export.ExportToS3NeptuneExportEventHandler;
import com.amazonaws.services.neptune.export.NeptuneExportServiceEventHandler;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.PropertyName;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.PropertyGraphTrainingDataConfigWriterV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.TrainingDataWriterConfigV1;
import com.amazonaws.services.neptune.propertygraph.EdgeLabelStrategy;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.io.CsvPrinterOptions;
import com.amazonaws.services.neptune.propertygraph.io.JsonPrinterOptions;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
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

import static com.amazonaws.services.neptune.export.NeptuneExportService.NEPTUNE_ML_PROFILE_NAME;

public class NeptuneMachineLearningExportEventHandlerV1 implements NeptuneExportServiceEventHandler {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeptuneMachineLearningExportEventHandlerV1.class);

    private final String outputS3Path;
    private final String s3Region;
    private final Args args;
    private final Collection<TrainingDataWriterConfigV1> trainingJobWriterConfigCollection;
    private final Collection<String> profiles;
    private final boolean createExportSubdirectory;
    private final PrinterOptions printerOptions;

    public NeptuneMachineLearningExportEventHandlerV1(String outputS3Path,
                                                      String s3Region,
                                                      boolean createExportSubdirectory,
                                                      ObjectNode additionalParams,
                                                      Args args,
                                                      Collection<String> profiles) {
        logger.info("Adding neptune_ml event handler");

        CsvPrinterOptions csvPrinterOptions = CsvPrinterOptions.builder()
                .setMultiValueSeparator(";")
                .setEscapeCsvHeaders(args.contains("--escape-csv-headers"))
                .build();
        JsonPrinterOptions jsonPrinterOptions = JsonPrinterOptions.builder()
                .setStrictCardinality(true)
                .build();

        this.outputS3Path = outputS3Path;
        this.s3Region = s3Region;
        this.createExportSubdirectory = createExportSubdirectory;
        this.args = args;
        this.trainingJobWriterConfigCollection = createTrainingJobConfigCollection(additionalParams);
        this.profiles = profiles;
        this.printerOptions = new PrinterOptions(csvPrinterOptions, jsonPrinterOptions);
    }

    private Collection<TrainingDataWriterConfigV1> createTrainingJobConfigCollection(ObjectNode additionalParams) {
        JsonNode neptuneMlNode = additionalParams.path(NEPTUNE_ML_PROFILE_NAME);
        if (neptuneMlNode.isMissingNode()) {
            logger.info("No 'neptune_ml' config node in additional params so creating default training config");
            return Collections.singletonList(new TrainingDataWriterConfigV1());
        } else {
            Collection<TrainingDataWriterConfigV1> trainingJobWriterConfig = TrainingDataWriterConfigV1.fromJson(neptuneMlNode);
            logger.info("Training job writer config: {}", trainingJobWriterConfig);
            return trainingJobWriterConfig;
        }
    }

    @Override
    public void onBeforeExport(Args args, ExportToS3NeptuneExportEventHandler.S3UploadParams s3UploadParams) {

        if (args.contains("export-pg")) {

            if (!args.contains("--exclude-type-definitions")) {
                args.addFlag("--exclude-type-definitions");
            }

            if (args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeLabelsOnly.name())) {
                args.removeOptions("--edge-label-strategy");
            }

            if (!args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name())) {
                args.addOption("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name());
            }


            if (args.containsAny("--config", "--filter", "-c", "--config-file", "--filter-config-file")){
                args.replace("export-pg", "export-pg-from-config");
            }

            if (!args.contains("--merge-files")) {
                args.addFlag("--merge-files");
            }
        }

        if (args.contains("--export-id")) {
            args.removeOptions("--export-id");
        }

        args.addOption("--export-id", new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
    }

    @Override
    public void onError() {
        // Do nothing
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster) throws Exception {
        //Do nothing
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster, GraphSchema graphSchema) throws Exception {

        PropertyName propertyName = args.contains("--exclude-type-definitions") ?
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE :
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITH_DATATYPE;

        try (TransferManagerWrapper transferManager = new TransferManagerWrapper(s3Region)) {
            for (TrainingDataWriterConfigV1 trainingJobWriterConfig : trainingJobWriterConfigCollection) {
                createTrainingJobConfigurationFile(trainingJobWriterConfig, directories.rootDirectory(), graphSchema, propertyName, transferManager);
            }
        }
    }

    private void createTrainingJobConfigurationFile(TrainingDataWriterConfigV1 trainingJobWriterConfig,
                                                    Path outputPath,
                                                    GraphSchema graphSchema,
                                                    PropertyName propertyName,
                                                    TransferManagerWrapper transferManager) throws Exception {

        File outputDirectory = outputPath.toFile();
        String filename = String.format("%s.json", trainingJobWriterConfig.name());
        File trainingJobConfigurationFile = new File(outputPath.toFile(), filename);

        try (Writer writer = new PrintWriter(trainingJobConfigurationFile)) {
            new PropertyGraphTrainingDataConfigWriterV1(
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
        if (createExportSubdirectory) {
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
