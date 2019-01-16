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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
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
                forEachRemaining(m -> {
                    try {
                        handler.handle(m, false);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void queryForValues(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter) {
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
