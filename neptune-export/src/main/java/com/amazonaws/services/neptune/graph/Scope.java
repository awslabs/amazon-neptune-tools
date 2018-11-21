package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.metadata.MetadataSpecification;
import com.amazonaws.services.neptune.metadata.MetadataTypes;

import java.util.*;
import java.util.stream.Collectors;

public enum Scope {
    all {
        @Override
        public Collection<MetadataSpecification<?>> metadataSpecifications(List<String> nodeLabels,
                                                                           List<String> edgeLabels) {
            return Arrays.asList(
                    new MetadataSpecification<>(MetadataTypes.Nodes, Scope.labelsFilter(nodeLabels)),
                    new MetadataSpecification<>(MetadataTypes.Edges, Scope.labelsFilter(edgeLabels))
            );
        }
    },
    nodes {
        @Override
        public Collection<MetadataSpecification<?>> metadataSpecifications(List<String> nodeLabels,
                                                                           List<String> edgeLabels) {
            return Collections.singletonList(
                    new MetadataSpecification<>(MetadataTypes.Nodes, Scope.labelsFilter(nodeLabels))
            );
        }
    },
    edges {
        @Override
        public Collection<MetadataSpecification<?>> metadataSpecifications(List<String> nodeLabels,
                                                                           List<String> edgeLabels) {
            return Collections.singletonList(
                    new MetadataSpecification<>(MetadataTypes.Edges, Scope.labelsFilter(edgeLabels))
            );
        }
    };

    private static Set<String> toSet(Collection<String> labels) {
        return labels.stream().flatMap(v -> Arrays.stream(v.split(","))).collect(Collectors.toSet());
    }

    private static LabelsFilter labelsFilter(Collection<String> labels){
        if (labels.isEmpty()){
            return AllLabels.INSTANCE;
        }

        return new SpecifiedLabels(toSet(labels));
    }

    public abstract Collection<MetadataSpecification<?>> metadataSpecifications(
            List<String> nodeLabels,
            List<String> edgeLabels);

}
