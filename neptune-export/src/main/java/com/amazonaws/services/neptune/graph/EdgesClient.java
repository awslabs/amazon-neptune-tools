/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.io.GraphElementHandler;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.IOException;
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
                forEachRemaining(m -> {
                    try {
                        handler.handle(m, false);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
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
        traversal.forEachRemaining(p -> {
            try {
                handler.handle(p, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
