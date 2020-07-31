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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;


public class NodesClient implements GraphClient<Map<String, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(NodesClient.class);

    private final GraphTraversalSource g;
    private final boolean tokensOnly;
    private final ExportStats stats;
    private final Collection<String> labModeFeatures;

    public NodesClient(GraphTraversalSource g,
                       boolean tokensOnly,
                       ExportStats stats,
                       Collection<String> labModeFeatures) {
        this.g = g;
        this.tokensOnly = tokensOnly;
        this.stats = stats;
        this.labModeFeatures = labModeFeatures;
    }

    @Override
    public String description() {
        return "node";
    }

    @Override
    public void queryForMetadata(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter) {

        GraphTraversal<? extends Element, Map<Object, Object>> t = tokensOnly ?
                createTraversal(range, labelsFilter).valueMap(true, "~TOKENS-ONLY") :
                createTraversal(range, labelsFilter).valueMap(true);

        logger.info(GremlinQueryDebugger.queryAsString(t));

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

        GraphTraversal<? extends Element, ?> traversal = createTraversal(range, labelsFilter);

        traversal = filterByPropertyKeys(traversal, labelsFilter, propertiesMetadata);

        GraphTraversal<? extends Element, Map<String, Object>> t = traversal.
                project("id", "label", "properties").
                by(T.id).
                by(label().fold()).
                by(tokensOnly ?
                        select("x") :
                        valueMap(labelsFilter.getPropertiesForLabels(propertiesMetadata))
                );

        logger.info(GremlinQueryDebugger.queryAsString(t));

        t.forEachRemaining(m -> {
            try {
                handler.handle(m, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private GraphTraversal<? extends Element, ?> filterByPropertyKeys(GraphTraversal<? extends Element, ?> traversal,
                                                                      LabelsFilter labelsFilter,
                                                                      PropertiesMetadata propertiesMetadata) {
        if (!labModeFeatures.contains("filterByPropertyKeys")) {
            return traversal;
        }

        return traversal.where(
                properties().key().is(P.within(labelsFilter.getPropertiesForLabels(propertiesMetadata))));
    }

    @Override
    public long approxCount(LabelsFilter labelsFilter, RangeConfig rangeConfig) {
        if (rangeConfig.approxNodeCount() > 0) {
            return rangeConfig.approxNodeCount();
        }

        GraphTraversal<? extends Element, Long> t = createTraversal(Range.ALL, labelsFilter).count();

        logger.info(GremlinQueryDebugger.queryAsString(t));

        Long count = t.next();
        stats.setNodeCount(count);
        return count;
    }

    @Override
    public Collection<String> labels() {
        // Using dedup can cause MemoryLimitExceededException on large datasets, so do the dedup in the set

        GraphTraversal<Vertex, String> traversal = g.V().label();

        logger.info(GremlinQueryDebugger.queryAsString(traversal));

        Set<String> labels = new HashSet<>();
        traversal.forEachRemaining(labels::add);
        return labels;
    }

    @Override
    public String getLabelsAsStringToken(Map<String, Object> input) {

        List<String> labels = (List<String>) input.get("label");
        if (labels.size() > 1) {
            return labels.toString();
        } else {
            return labels.get(0);
        }
    }

    @Override
    public void updateStats(String label) {
        stats.incrementNodeStats(label);
    }

    private GraphTraversal<? extends Element, ?> createTraversal(Range range, LabelsFilter labelsFilter) {
        GraphTraversal<Vertex, Vertex> t = tokensOnly ?
                g.withSideEffect("x", new HashMap<String, Object>()).V() :
                g.V();
        return range.applyRange(labelsFilter.apply(t));
    }

}
