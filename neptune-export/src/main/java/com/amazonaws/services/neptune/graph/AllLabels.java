package com.amazonaws.services.neptune.graph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;

public class AllLabels implements LabelsFilter {

    public static final LabelsFilter INSTANCE = new AllLabels();

    private AllLabels(){}

    @Override
    public GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal) {
        return traversal;
    }

    @Override
    public Collection<String> resolveLabels(GraphClient<?> graphClient) {
        return graphClient.labels();
    }
}
