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

package com.amazonaws.services.neptune.propertygraph;

import com.amazonaws.services.neptune.propertygraph.io.GraphElementHandler;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadata;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.IOException;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class EdgesClient implements GraphClient<Map<String, Object>> {

    private final GraphTraversalSource g;
    private final boolean tokensOnly;
    private final ExportStats stats;

    public EdgesClient(GraphTraversalSource g, boolean tokensOnly, ExportStats stats) {
        this.g = g;
        this.tokensOnly = tokensOnly;
        this.stats = stats;
    }

    @Override
    public String description() {
        return "edge";
    }

    @Override
    public void queryForMetadata(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter) {
        GraphTraversal<? extends Element, Map<Object, Object>> t = tokensOnly ?
                traversal(range, labelsFilter).valueMap(true, "~TOKENS-ONLY") :
                traversal(range, labelsFilter).valueMap(true);

        t.forEachRemaining(m -> {
            try {
                handler.handle(m, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void queryForValues(GraphElementHandler<Map<String, Object>> handler,
                               Range range,
                               LabelsFilter labelsFilter,
                               PropertiesMetadata propertiesMetadata) {

        GraphTraversal<Edge, Edge> t = tokensOnly ?
                g.withSideEffect("x", new HashMap<String, Object>()).E():
                g.E();


        GraphTraversal<? extends Element, Map<String, Object>> traversal =
                range.applyRange(labelsFilter.apply(t)).
                        project("id", "label", "properties", "from", "to").
                        by(T.id).
                        by(T.label).
                        by(tokensOnly ?
                                select("x"):
                                valueMap(labelsFilter.getPropertiesForLabels(propertiesMetadata))
                        ).
                        by(outV().id()).
                        by(inV().id());

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
        Long count = traversal(Range.ALL, labelsFilter).count().next();
        stats.setEdgeCount(count);
        return count;
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
    public String getLabelFrom(Map<String, Object> input) {
        return (String) input.get("label");
    }

    @Override
    public void updateStats(String label) {
        stats.incrementEdgeStats(label);
    }

    private GraphTraversal<? extends Element, ?> traversal(Range range, LabelsFilter labelsFilter) {
        return range.applyRange(labelsFilter.apply(g.E()));
    }
}
