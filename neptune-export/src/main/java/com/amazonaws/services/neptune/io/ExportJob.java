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

package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.propertygraph.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.RangeFactory;
import com.amazonaws.services.neptune.metadata.MetadataSpecification;
import com.amazonaws.services.neptune.metadata.PropertiesMetadataCollection;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExportJob {
    private final Collection<MetadataSpecification<?>> metadataSpecifications;
    private final PropertiesMetadataCollection propertiesMetadataCollection;
    private final GraphTraversalSource g;
    private final ConcurrencyConfig concurrencyConfig;
    private final Directories directories;
    private final Format format;

    public ExportJob(Collection<MetadataSpecification<?>> metadataSpecifications,
                     PropertiesMetadataCollection propertiesMetadataCollection,
                     GraphTraversalSource g,
                     ConcurrencyConfig concurrencyConfig,
                     Directories directories,
                     Format format) {
        this.metadataSpecifications = metadataSpecifications;
        this.propertiesMetadataCollection = propertiesMetadataCollection;
        this.g = g;
        this.concurrencyConfig = concurrencyConfig;
        this.directories = directories;
        this.format = format;
    }

    public void execute() throws Exception {

        for (MetadataSpecification metadataSpecification : metadataSpecifications) {

            try (Timer timer = new Timer()) {
                System.err.println("Creating " + metadataSpecification.description() + " " + format.description() + " files");

                RangeFactory rangeFactory = metadataSpecification.createRangeFactory(g, concurrencyConfig);
                Status status = new Status();

                ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

                for (int index = 1; index <= concurrencyConfig.concurrency(); index++) {
                    ExportTask exportTask = metadataSpecification.createExportTask(
                            propertiesMetadataCollection,
                            g,
                            directories,
                            format,
                            rangeFactory,
                            status,
                            index);
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
