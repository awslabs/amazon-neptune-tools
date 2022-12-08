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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RewriteCsv implements RewriteCommand {

    private static final Logger logger = LoggerFactory.getLogger(RewriteCsv.class);

    private final PropertyGraphTargetConfig targetConfig;
    private final ConcurrencyConfig concurrencyConfig;
    private final FeatureToggles featureToggles;

    public RewriteCsv(PropertyGraphTargetConfig targetConfig,
                      ConcurrencyConfig concurrencyConfig,
                      FeatureToggles featureToggles) {
        this.targetConfig = targetConfig;
        this.concurrencyConfig = concurrencyConfig;
        this.featureToggles = featureToggles;
    }

    @Override
    public MasterLabelSchemas execute(MasterLabelSchemas masterLabelSchemas) throws Exception {
        GraphElementType graphElementType = masterLabelSchemas.graphElementType();

        System.err.println(String.format("Rewriting %s files...", graphElementType.name()));

        return Timer.timedActivity(String.format("rewriting %s files", graphElementType.name()),
                (CheckedActivity.Callable<MasterLabelSchemas>) () ->
                        rewriteFiles(masterLabelSchemas, graphElementType, targetConfig));
    }

    private MasterLabelSchemas rewriteFiles(MasterLabelSchemas masterLabelSchemas,
                                            GraphElementType graphElementType,
                                            PropertyGraphTargetConfig targetConfig) throws Exception {

        Map<Label, MasterLabelSchema> updatedSchemas = new HashMap<>();

        Collection<Future<MasterLabelSchema>> futures = new ArrayList<>();
        ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

        for (MasterLabelSchema masterLabelSchema : masterLabelSchemas.schemas()) {
            futures.add(taskExecutor.submit(() -> rewrite(targetConfig, graphElementType, masterLabelSchema)));
        }
        taskExecutor.shutdown();

        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        for (Future<MasterLabelSchema> future : futures) {
            if (future.isCancelled()) {
                throw new IllegalStateException("Unable to complete rewrite because at least one task was cancelled");
            }
            if (!future.isDone()) {
                throw new IllegalStateException("Unable to complete rewrite because at least one task has not completed");
            }
            MasterLabelSchema masterLabelSchema = future.get();
            updatedSchemas.put(masterLabelSchema.labelSchema().label(), masterLabelSchema);
        }

        return new MasterLabelSchemas(updatedSchemas, graphElementType);
    }

    private MasterLabelSchema rewrite(PropertyGraphTargetConfig targetConfig,
                                      GraphElementType graphElementType,
                                      MasterLabelSchema masterLabelSchema) throws Exception {

        LabelSchema masterSchema = masterLabelSchema.labelSchema().createCopy();
        masterSchema.initStats();

        Collection<String> renamedFiles = new ArrayList<>();

        for (FileSpecificLabelSchema fileSpecificLabelSchema : masterLabelSchema.fileSpecificLabelSchemas()) {

            LabelSchema labelSchema = fileSpecificLabelSchema.labelSchema();
            Label label = labelSchema.label();
            File sourceCsvFile = new File(fileSpecificLabelSchema.outputId());


            if (!sourceCsvFile.exists()) {
                if (label.label().size() > 1) {
                    logger.warn("Skipping multi-label file {} because it has already been rewritten under another label", sourceCsvFile);
                    continue;
                }
            }

            String[] additionalElementHeaders = label.hasFromAndToLabels() ?
                    new String[]{"~fromLabels", "~toLabels"} :
                    new String[]{};

            String[] filePropertyHeaders =
                    labelSchema.propertySchemas().stream()
                            .map(p -> p.property().toString())
                            .collect(Collectors.toList())
                            .toArray(new String[]{});

            String[] fileHeaders = ArrayUtils.addAll(
                    graphElementType.tokenNames().toArray(new String[]{}),
                    ArrayUtils.addAll(additionalElementHeaders, filePropertyHeaders));

            try (DeletableFile sourceFile = new DeletableFile(sourceCsvFile);
                 Reader in = sourceFile.reader();
                 PropertyGraphPrinter target = graphElementType.writerFactory().createPrinter(
                         targetConfig.format().replaceExtension(sourceCsvFile.getName(), "modified"),
                         masterSchema,
                         targetConfig.forFileConsolidation());
            ) {

                if (featureToggles.containsFeature(FeatureToggle.Keep_Rewritten_Files)){
                    sourceFile.doNotDelete();
                }

                renamedFiles.add(target.outputId());

                CSVFormat format = CSVFormat.RFC4180.withHeader(fileHeaders);
                Iterable<CSVRecord> records = format.parse(in);

                int recordCount = 0;

                for (CSVRecord record : records) {
                    target.printStartRow();

                    if (graphElementType == GraphElementType.nodes) {
                        target.printNode(record.get("~id"), Arrays.asList(record.get("~label").split(";")));
                    } else {
                        if (label.hasFromAndToLabels()) {
                            target.printEdge(
                                    record.get("~id"),
                                    record.get("~label"),
                                    record.get("~from"),
                                    record.get("~to"),
                                    Arrays.asList(record.get("~fromLabels").split(";")),
                                    Arrays.asList(record.get("~toLabels").split(";")));
                        } else {
                            target.printEdge(record.get("~id"), record.get("~label"), record.get("~from"), record.get("~to"));
                        }
                    }

                    target.printProperties(record.toMap(), false);
                    target.printEndRow();

                    recordCount++;
                }

                logger.info("Original: {}, Rewritten: {}, RecordCount: {}", sourceFile, target.outputId(), recordCount);
            }

        }

        return new MasterLabelSchema(
                masterSchema,
                renamedFiles.stream().map(f -> new FileSpecificLabelSchema(f, targetConfig.format(), masterSchema)).collect(Collectors.toList()));
    }

}
