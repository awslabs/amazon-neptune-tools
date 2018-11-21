package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.io.GraphElementHandler;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;

public class EdgesClient implements GraphClient<Path> {

    private final GraphTraversalSource g;

    public EdgesClient(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public String description() {
        return "edge";
    }

    @Override
    public void queryForMetadata(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter) {
        traversal(range, labelsFilter).valueMap(true).
                forEachRemaining(m -> handler.handle(m, false));
    }

    @Override
    public void queryForValues(GraphElementHandler<Path> handler, Range range, LabelsFilter labelsFilter) {
        GraphTraversal<? extends Element, Path> traversal = range.applyRange(labelsFilter.apply(g.E())).as("e").
                inV().select("e").outV().
                path().
                by(valueMap(true)).
                by(__.id()).
                by(__.id()).
                by(__.id());
        traversal.forEachRemaining(p -> handler.handle(p, false));
    }

    @Override
    public long count(LabelsFilter labelsFilter) {
        return traversal(Range.ALL, labelsFilter).count().next();
    }

    @Override
    public Collection<String> labels() {
        // Using dedup can cause MemoryLimitExceededException on large datasets, so do the dedup in the set

        GraphTraversal<Edge, String> traversal = g.E().label();
        Set<String> labels = new HashSet<>();
        traversal.forEachRemaining(labels::add);
        return labels;
    }

    @Override
    public String getLabelFrom(Path input) {
        Map<?, Object> properties = input.get(0);
        return String.valueOf(properties.get(T.label));
    }

    private GraphTraversal<? extends Element, ?> traversal(Range range, LabelsFilter labelsFilter) {
        return range.applyRange(labelsFilter.apply(g.E()));
    }
}
