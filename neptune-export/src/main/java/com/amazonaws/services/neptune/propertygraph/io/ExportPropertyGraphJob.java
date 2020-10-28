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

import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.RangeConfig;
import com.amazonaws.services.neptune.propertygraph.RangeFactory;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExportPropertyGraphJob {
    private final Collection<ExportSpecification<?>> exportSpecifications;
    private final GraphSchema graphSchema;
    private final GraphTraversalSource g;
    private final RangeConfig rangeConfig;
    private final ConcurrencyConfig concurrencyConfig;
    private final PropertyGraphTargetConfig targetConfig;

    public ExportPropertyGraphJob(Collection<ExportSpecification<?>> exportSpecifications,
                                  GraphSchema graphSchema,
                                  GraphTraversalSource g,
                                  RangeConfig rangeConfig,
                                  ConcurrencyConfig concurrencyConfig,
                                  PropertyGraphTargetConfig targetConfig) {
        this.exportSpecifications = exportSpecifications;
        this.graphSchema = graphSchema;
        this.g = g;
        this.rangeConfig = rangeConfig;
        this.concurrencyConfig = concurrencyConfig;
        this.targetConfig = targetConfig;
    }

    public GraphSchema execute() throws Exception {
        Map<GraphElementType<?>, GraphElementSchemas> revisedGraphElementSchemas = new HashMap<>();

        for (ExportSpecification<?> exportSpecification : exportSpecifications) {
            MasterLabelSchemas masterLabelSchemas =
                    Timer.timedActivity("exporting " + exportSpecification.description(),
                            (CheckedActivity.Callable<MasterLabelSchemas>) () -> export(exportSpecification));
            revisedGraphElementSchemas.put(masterLabelSchemas.graphElementType(), masterLabelSchemas.toGraphElementSchemas());
        }

        return new GraphSchema(revisedGraphElementSchemas);
    }

    private MasterLabelSchemas export(ExportSpecification<?> exportSpecification) throws Exception {

        Collection<Future<FileSpecificLabelSchemas>> futures = new ArrayList<>();

        System.err.println("Writing " + exportSpecification.description() + " as " + targetConfig.format().description() + " to " + targetConfig.output().name());

        RangeFactory rangeFactory = exportSpecification.createRangeFactory(g, rangeConfig, concurrencyConfig);
        Status status = new Status();

        ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

        for (int index = 1; index <= concurrencyConfig.concurrency(); index++) {
            ExportPropertyGraphTask<?> exportTask = exportSpecification.createExportTask(
                    graphSchema,
                    g,
                    targetConfig,
                    rangeFactory,
                    status,
                    index
            );
            futures.add(taskExecutor.submit(exportTask));
        }

        taskExecutor.shutdown();

        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        Collection<FileSpecificLabelSchemas> fileSpecificLabelSchemas = getFileSpecificLabelSchemas(futures);
        MasterLabelSchemas masterLabelSchemas = exportSpecification.createMasterLabelSchemas(fileSpecificLabelSchemas);

        RewriteCommand rewriteCommand = targetConfig.createRewriteCommand();

        return rewriteCommand.execute(masterLabelSchemas);
    }

    private Collection<FileSpecificLabelSchemas> getFileSpecificLabelSchemas(
            Collection<Future<FileSpecificLabelSchemas>> futures) throws Exception {

        Collection<FileSpecificLabelSchemas> allFileSpecificLabelSchemas = new ArrayList<>();

        for (Future<FileSpecificLabelSchemas> future : futures) {
            if (future.isCancelled()) {
                throw new IllegalStateException("Unable to complete job because at least one task was cancelled");
            }
            if (!future.isDone()) {
                throw new IllegalStateException("Unable to complete job because at least one task has not completed");
            }
            allFileSpecificLabelSchemas.add(future.get());
        }

        return allFileSpecificLabelSchemas;
    }
}
