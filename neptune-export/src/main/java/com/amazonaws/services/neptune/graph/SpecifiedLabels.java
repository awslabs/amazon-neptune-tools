package com.amazonaws.services.neptune.graph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class SpecifiedLabels implements LabelsFilter {

    public static LabelsFilter forLabels(String... labels){
        Set<String> set = new HashSet<>();
        Collections.addAll(set, labels);
        return new SpecifiedLabels(set);
    }

    private final Set<String> labels;

    public SpecifiedLabels(Set<String> labels) {
        this.labels = labels;
    }

    @Override
    public GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal) {
        String firstLabel = labels.stream().findFirst().orElse(null);
        String[] remainingLabels = labels.stream().skip(1).collect(Collectors.toList()).toArray(new String[]{});

        return traversal.hasLabel(firstLabel, remainingLabels);
    }

    @Override
    public Collection<String> resolveLabels(GraphClient<?> graphClient) {
        return labels;
    }
}
