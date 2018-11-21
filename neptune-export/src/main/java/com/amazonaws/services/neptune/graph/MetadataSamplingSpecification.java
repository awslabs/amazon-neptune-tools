package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.metadata.CreateMetadataFromGraphSample;
import com.amazonaws.services.neptune.metadata.CreateMetadataFromGraphScan;
import com.amazonaws.services.neptune.metadata.MetadataCommand;
import com.amazonaws.services.neptune.metadata.MetadataSpecification;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class MetadataSamplingSpecification {

    private final boolean sample;
    private final long sampleSize;

    public MetadataSamplingSpecification(boolean sample, long sampleSize) {
        this.sample = sample;
        this.sampleSize = sampleSize;
    }

    public MetadataCommand createMetadataCommand(Collection<MetadataSpecification<?>> metadataSpecifications,
                                                 GraphTraversalSource g) {
        if (sample) {
            return new CreateMetadataFromGraphSample(metadataSpecifications, g, sampleSize);
        } else {
            return new CreateMetadataFromGraphScan(metadataSpecifications, g);
        }
    }
}
