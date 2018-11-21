package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.graph.ConcurrencyConfig;
import com.amazonaws.services.neptune.graph.RangeFactory;
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

    public ExportJob(Collection<MetadataSpecification<?>> metadataSpecifications,
                     PropertiesMetadataCollection propertiesMetadataCollection,
                     GraphTraversalSource g,
                     ConcurrencyConfig concurrencyConfig,
                     Directories directories) {
        this.metadataSpecifications = metadataSpecifications;
        this.propertiesMetadataCollection = propertiesMetadataCollection;
        this.g = g;
        this.concurrencyConfig = concurrencyConfig;
        this.directories = directories;
    }

    public void execute() throws Exception {

        for (MetadataSpecification metadataSpecification : metadataSpecifications) {

            try (Timer timer = new Timer()) {
                System.err.println("Creating " + metadataSpecification.description() + " CSV files");

                RangeFactory rangeFactory = metadataSpecification.createRangeFactory(g, concurrencyConfig);
                Status status = new Status();

                ExecutorService taskExecutor = Executors.newFixedThreadPool(concurrencyConfig.concurrency());

                for (int index = 1; index <= concurrencyConfig.concurrency(); index++) {
                    ExportTask exportTask = metadataSpecification.createExportTask(
                            propertiesMetadataCollection,
                            g,
                            directories,
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
