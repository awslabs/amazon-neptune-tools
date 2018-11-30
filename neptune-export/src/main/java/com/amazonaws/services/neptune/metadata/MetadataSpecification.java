package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.graph.ConcurrencyConfig;
import com.amazonaws.services.neptune.graph.Range;
import com.amazonaws.services.neptune.graph.RangeFactory;
import com.amazonaws.services.neptune.graph.GraphClient;
import com.amazonaws.services.neptune.graph.LabelsFilter;
import com.amazonaws.services.neptune.graph.SpecifiedLabels;
import com.amazonaws.services.neptune.io.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.Map;

public class MetadataSpecification<T> {
    private final MetadataType<T> metadataType;
    private final LabelsFilter labelsFilter;

    public MetadataSpecification(MetadataType<T> metadataType, LabelsFilter labelsFilter) {
        this.metadataType = metadataType;
        this.labelsFilter = labelsFilter;
    }

    public void scan(PropertiesMetadataCollection metadataCollection, GraphTraversalSource g) {
        GraphClient<T> graphClient = metadataType.graphClient(g);

        graphClient.queryForMetadata(
                new Handler(metadataType, metadataCollection),
                Range.ALL,
                labelsFilter);
    }

    public void sample(PropertiesMetadataCollection metadataCollection, GraphTraversalSource g, long sampleSize) {
        GraphClient<T> graphClient = metadataType.graphClient(g);
        Collection<String> labels = labelsFilter.resolveLabels(graphClient);

        for (String label : labels) {
            graphClient.queryForMetadata(
                    new Handler(metadataType, metadataCollection),
                    new Range(0, sampleSize),
                    SpecifiedLabels.forLabels(label));
        }
    }

    public String description(){
        return metadataType.name();
    }

    public RangeFactory createRangeFactory(GraphTraversalSource g, ConcurrencyConfig concurrencyConfig) {
        return RangeFactory.create(metadataType.graphClient(g), labelsFilter, concurrencyConfig);
    }

    public ExportTask<T> createExportTask(PropertiesMetadataCollection metadataCollection,
                                          GraphTraversalSource g,
                                          Directories directories,
                                          Format format,
                                          RangeFactory rangeFactory,
                                          Status status,
                                          int index) {
        return new ExportTask<>(
                metadataCollection.propertyMetadataFor(metadataType),
                labelsFilter,
                metadataType.graphClient(g),
                metadataType.writerFactory(directories),
                format,
                rangeFactory,
                status,
                index
        );
    }

    private static class Handler implements GraphElementHandler<Map<?, Object>> {

        private final MetadataType metadataType;
        private final PropertiesMetadataCollection metadataCollection;
        private final Status status = new Status();

        private Handler(MetadataType metadataType, PropertiesMetadataCollection metadataCollection) {
            this.metadataType = metadataType;
            this.metadataCollection = metadataCollection;
        }

        @Override
        public void handle(Map<?, Object> properties, boolean allowStructuralElements) {
            status.update();
            metadataCollection.update(metadataType, properties, allowStructuralElements);
        }

        @Override
        public void close() throws Exception {
            // Do nothing
        }
    }
}
