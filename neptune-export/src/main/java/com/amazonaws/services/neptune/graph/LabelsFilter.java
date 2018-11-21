package com.amazonaws.services.neptune.graph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;

public interface LabelsFilter {
    GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal);

    Collection<String> resolveLabels(GraphClient<?> graphClient);
}
