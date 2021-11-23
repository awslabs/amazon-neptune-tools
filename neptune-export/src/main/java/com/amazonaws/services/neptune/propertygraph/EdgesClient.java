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

import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.propertygraph.io.GraphElementHandler;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementType;
import com.amazonaws.services.neptune.util.Activity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class EdgesClient implements GraphClient<Map<String, Object>> {

    private static final Logger logger = LoggerFactory.getLogger(EdgesClient.class);

    private final GraphTraversalSource g;
    private final boolean tokensOnly;
    private final ExportStats stats;
    private final FeatureToggles featureToggles;

    public EdgesClient(GraphTraversalSource g,
                       boolean tokensOnly,
                       ExportStats stats,
                       FeatureToggles featureToggles) {
        this.g = g;
        this.tokensOnly = tokensOnly;
        this.stats = stats;
        this.featureToggles = featureToggles;
    }

    @Override
    public String description() {
        return "edge";
    }

    @Override
    public void queryForSchema(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter, GremlinFilters gremlinFilters) {
        GraphTraversal<? extends Element, Map<Object, Object>> t1 = tokensOnly ?
                traversal(range, labelsFilter).valueMap(true, "~TOKENS-ONLY") :
                traversal(range, labelsFilter).valueMap(true);

        logger.info(GremlinQueryDebugger.queryAsString(t1));

        t1.forEachRemaining(m -> {
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
                               GremlinFilters gremlinFilters,
                               GraphElementSchemas graphElementSchemas) {

        GraphTraversal<Edge, Edge> t1 = tokensOnly ?
                g.withSideEffect("x", new HashMap<String, Object>()).E() :
                g.E();

        GraphTraversal<? extends Element, ?> t2 = range.applyRange(labelsFilter.apply(t1, featureToggles, GraphElementType.edges));
        GraphTraversal<? extends Element, ?> t3 = filterByPropertyKeys(t2, labelsFilter, graphElementSchemas);

        GraphTraversal<? extends Element, ?> t4 = gremlinFilters.applyToEdges(t3);

        GraphTraversal<? extends Element, Map<String, Object>> t5 = t4.
                project("~id", labelsFilter.addAdditionalColumnNames("~label", "properties", "~from", "~to")).
                by(T.id).
                by(T.label).
                by(tokensOnly ?
                        select("x") :
                        valueMap(labelsFilter.getPropertiesForLabels(graphElementSchemas))
                ).
                by(outV().id()).
                by(inV().id());

        GraphTraversal<? extends Element, Map<String, Object>> traversal = labelsFilter.addAdditionalColumns(t5);

        logger.info(GremlinQueryDebugger.queryAsString(traversal));

        traversal.forEachRemaining(p -> {
            try {
                if (featureToggles.containsFeature(FeatureToggle.Inject_Fault)){
                    throw new IllegalStateException("Simulated fault in EdgesClient");
                }
                handler.handle(p, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private GraphTraversal<? extends Element, ?> filterByPropertyKeys(GraphTraversal<? extends Element, ?> traversal,
                                                                      LabelsFilter labelsFilter,
                                                                      GraphElementSchemas graphElementSchemas) {
        if (!featureToggles.containsFeature(FeatureToggle.FilterByPropertyKeys)) {
            return traversal;
        }

        return traversal.where(
                properties().key().is(P.within(labelsFilter.getPropertiesForLabels(graphElementSchemas))));
    }

    @Override
    public long approxCount(LabelsFilter labelsFilter, RangeConfig rangeConfig, GremlinFilters gremlinFilters) {

        if (rangeConfig.approxEdgeCount() > 0) {
            return rangeConfig.approxEdgeCount();
        }

        String description = labelsFilter.description("edges");
        System.err.println(String.format("Counting %s...", description));

        return Timer.timedActivity(String.format("counting %s", description), (Activity.Callable<Long>) () -> {
            GraphTraversal<? extends Element, Long> t = traversal(Range.ALL, labelsFilter).count();
            logger.info(GremlinQueryDebugger.queryAsString(t));

            Long count = t.next();

            stats.setEdgeCount(count);
            return count;
        });
    }

    @Override
    public Collection<Label> labels(LabelStrategy labelStrategy) {
        return labelStrategy.getLabels(g);
    }

    @Override
    public Label getLabelFor(Map<String, Object> input, LabelsFilter labelsFilter) {
        return labelsFilter.getLabelFor(input);
    }

    @Override
    public void updateStats(Label label) {
        stats.incrementEdgeStats(label);
    }

    private GraphTraversal<? extends Element, ?> traversal(Range range, LabelsFilter labelsFilter) {
        return range.applyRange(labelsFilter.apply(g.E(), featureToggles, GraphElementType.edges));
    }
}
