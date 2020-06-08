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
import com.amazonaws.services.neptune.propertygraph.metadata.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadataCollection;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExportPropertyGraphJob {
    private final Collection<ExportSpecification<?>> exportSpecifications;
    private final PropertiesMetadataCollection propertiesMetadataCollection;
    private final GraphTraversalSource g;
    private final RangeConfig rangeConfig;
    private final ConcurrencyConfig concurrencyConfig;
    private final PropertyGraphTargetConfig targetConfig;

    public ExportPropertyGraphJob(Collection<ExportSpecification<?>> exportSpecifications,
                                  PropertiesMetadataCollection propertiesMetadataCollection,
                                  GraphTraversalSource g,
                                  RangeConfig rangeConfig,
                                  ConcurrencyConfig concurrencyConfig,
                                  PropertyGraphTargetConfig targetConfig) {
        this.exportSpecifications = exportSpecifications;
        this.propertiesMetadataCollection = propertiesMetadataCollection;
        this.g = g;
        this.rangeConfig = rangeConfig;
        this.concurrencyConfig = concurrencyConfig;
        this.targetConfig = targetConfig;
    }

    public void execute() throws Exception {

        for (ExportSpecification<?> exportSpecification : exportSpecifications) {

            try (Timer timer = new Timer("exporting " + exportSpecification.description())) {
                System.err.println("Writing " + exportSpecification.description() + " as " + targetConfig.formatDescription() + " to " + targetConfig.outputDescription());

                RangeFactory rangeFactory = exportSpecification.createRangeFactory(g, rangeConfig, concurrencyConfig);
                Status status = new Status();

                ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

                for (int index = 1; index <= concurrencyConfig.concurrency(); index++) {
                    ExportPropertyGraphTask<?> exportTask = exportSpecification.createExportTask(
                            propertiesMetadataCollection,
                            g,
                            targetConfig,
                            rangeFactory,
                            status,
                            index
                    );
                    taskExecutor.execute(exportTask);
                }

                taskExecutor.shutdown();

                try {
                    taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
