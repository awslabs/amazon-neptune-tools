package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class CreateMetadataFromGraphSample implements MetadataCommand {

    private final Collection<MetadataSpecification<?>> metadataSpecifications;
    private final GraphTraversalSource g;
    private final long sampleSize;

    public CreateMetadataFromGraphSample(Collection<MetadataSpecification<?>> metadataSpecifications,
                                         GraphTraversalSource g,
                                         long sampleSize) {
        this.metadataSpecifications = metadataSpecifications;
        this.sampleSize = sampleSize;
        this.g = g;
    }

    @Override
    public PropertiesMetadataCollection execute() throws Exception {

        PropertiesMetadataCollection metadataCollection = new PropertiesMetadataCollection();
        for (MetadataSpecification metadataSpecification : metadataSpecifications) {
            try (Timer timer = new Timer()) {
                System.err.println("Creating " + metadataSpecification.description() + " metadata");
                metadataSpecification.sample(metadataCollection, g, sampleSize);
            }
        }
        return metadataCollection;
    }

}
