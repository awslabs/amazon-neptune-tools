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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.io.StatusOutputFormat;
import com.amazonaws.services.neptune.propertygraph.GremlinFilters;
import com.amazonaws.services.neptune.propertygraph.RangeConfig;
import com.amazonaws.services.neptune.propertygraph.RangeFactory;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExportPropertyGraphJob {

    private static final Logger logger = LoggerFactory.getLogger(ExportPropertyGraphJob.class);

    private final Collection<ExportSpecification> exportSpecifications;
    private final GraphSchema graphSchema;
    private final GraphTraversalSource g;
    private final RangeConfig rangeConfig;
    private final GremlinFilters gremlinFilters;
    private final ConcurrencyConfig concurrencyConfig;
    private final PropertyGraphTargetConfig targetConfig;
    private final FeatureToggles featureToggles;

    public ExportPropertyGraphJob(Collection<ExportSpecification> exportSpecifications,
                                  GraphSchema graphSchema,
                                  GraphTraversalSource g,
                                  RangeConfig rangeConfig,
                                  GremlinFilters gremlinFilters,
                                  ConcurrencyConfig concurrencyConfig,
                                  PropertyGraphTargetConfig targetConfig,
                                  FeatureToggles featureToggles) {
        this.exportSpecifications = exportSpecifications;
        this.graphSchema = graphSchema;
        this.g = g;
        this.rangeConfig = rangeConfig;
        this.gremlinFilters = gremlinFilters;
        this.concurrencyConfig = concurrencyConfig;
        this.targetConfig = targetConfig;
        this.featureToggles = featureToggles;
    }

    public GraphSchema execute() throws Exception {
        Map<GraphElementType, GraphElementSchemas> revisedGraphElementSchemas = new HashMap<>();

        for (ExportSpecification exportSpecification : exportSpecifications) {
            MasterLabelSchemas masterLabelSchemas =
                    Timer.timedActivity("exporting " + exportSpecification.description(),
                            (CheckedActivity.Callable<MasterLabelSchemas>) () -> export(exportSpecification));
            revisedGraphElementSchemas.put(masterLabelSchemas.graphElementType(), masterLabelSchemas.toGraphElementSchemas());
        }

        return new GraphSchema(revisedGraphElementSchemas);
    }

    private MasterLabelSchemas export(ExportSpecification exportSpecification) throws Exception {
        Collection<FileSpecificLabelSchemas> fileSpecificLabelSchemas = new ArrayList<>();

        AtomicInteger fileDescriptorCount = new AtomicInteger();


        for (ExportSpecification labelSpecificExportSpecification : exportSpecification.splitByLabel()) {
            Collection<Future<FileSpecificLabelSchemas>> futures = new ArrayList<>();
            RangeFactory rangeFactory = labelSpecificExportSpecification.createRangeFactory(g, rangeConfig, concurrencyConfig);
            Status status = new Status(StatusOutputFormat.Description, String.format("%s: %s total", labelSpecificExportSpecification.description(), rangeFactory.numberOfItemsToExport()));

            String description = String.format("writing %s as %s to %s",
                    labelSpecificExportSpecification.description(),
                    targetConfig.format().description(),
                    targetConfig.output().name());

            System.err.println("Started " + description);

            AtomicInteger fileIndex = new AtomicInteger();

            Timer.timedActivity(description, (CheckedActivity.Runnable) () -> {
                ExecutorService taskExecutor = Executors.newFixedThreadPool(rangeFactory.concurrency());

                for (int index = 1; index <= rangeFactory.concurrency(); index++) {
                    ExportPropertyGraphTask<?> exportTask = labelSpecificExportSpecification.createExportTask(
                            graphSchema,
                            g,
                            targetConfig,
                            gremlinFilters,
                            rangeFactory,
                            status,
                            fileIndex,
                            fileDescriptorCount
                    );
                    futures.add(taskExecutor.submit(exportTask));
                }

                taskExecutor.shutdown();

                try {
                    if (!taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                        logger.warn("Timeout expired with uncompleted tasks");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }

                updateFileSpecificLabelSchemas(futures, fileSpecificLabelSchemas);
            });
        }

        MasterLabelSchemas masterLabelSchemas = exportSpecification.createMasterLabelSchemas(fileSpecificLabelSchemas);

        RewriteCommand rewriteCommand = targetConfig.createRewriteCommand(concurrencyConfig, featureToggles);

        return rewriteCommand.execute(masterLabelSchemas);
    }

    private void updateFileSpecificLabelSchemas(
            Collection<Future<FileSpecificLabelSchemas>> futures,
            Collection<FileSpecificLabelSchemas> fileSpecificLabelSchemas) throws Exception {

        for (Future<FileSpecificLabelSchemas> future : futures) {
            if (future.isCancelled()) {
                throw new IllegalStateException("Unable to complete job because at least one task was cancelled");
            }
            if (!future.isDone()) {
                throw new IllegalStateException("Unable to complete job because at least one task has not completed");
            }
            fileSpecificLabelSchemas.add(future.get());
        }
    }
}
