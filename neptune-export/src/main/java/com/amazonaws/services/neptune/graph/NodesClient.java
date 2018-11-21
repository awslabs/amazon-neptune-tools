package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.io.GraphElementHandler;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NodesClient implements GraphClient<Map<?, Object>> {

    private final GraphTraversalSource g;

    public NodesClient(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public String description() {
        return "node";
    }

    @Override
    public void queryForMetadata(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter) {
        traversal(range, labelsFilter).valueMap(true).
                forEachRemaining(m -> handler.handle(m, false));
    }

    @Override
    public void queryForValues(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter) {
        traversal(range, labelsFilter).valueMap(true).
                forEachRemaining(m -> handler.handle(m, false));
    }

    @Override
    public long count(LabelsFilter labelsFilter) {
        return traversal(Range.ALL, labelsFilter).count().next();
    }

    @Override
    public Collection<String> labels() {
        // Using dedup can cause MemoryLimitExceededException on large datasets, so do the dedup in the set

        GraphTraversal<Vertex, String> traversal = g.V().label();
        Set<String> labels = new HashSet<>();
        traversal.forEachRemaining(labels::add);
        return labels;
    }

    @Override
    public String getLabelFrom(Map<?, Object> input) {
        return String.valueOf(input.get(T.label));
    }

    private GraphTraversal<? extends Element, ?> traversal(Range range, LabelsFilter labelsFilter) {
        return range.applyRange(labelsFilter.apply(g.V()));
    }
}
