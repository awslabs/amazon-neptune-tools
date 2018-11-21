package com.amazonaws.services.neptune.graph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

public class Range {

    public static final Range ALL = new Range(0, -1);

    private final long start;
    private final long end;

    public Range(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public GraphTraversal<? extends Element, ?> applyRange(GraphTraversal<? extends Element, ?> traversal) {
        if (end == -1) {
            return traversal;
        } else {
            return traversal.range(start, end);
        }
    }

    public long start() {
        return start;
    }

    public long value(){
        return end - start;
    }

    @Override
    public String toString() {
        return "range(" + start + ", " + end + ")";
    }
}
