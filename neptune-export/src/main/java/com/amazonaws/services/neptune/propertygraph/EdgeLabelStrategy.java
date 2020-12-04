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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public enum EdgeLabelStrategy implements LabelStrategy {

    edgeLabelsOnly {
        @Override
        public Collection<Label> getLabels(GraphTraversalSource g) {
            // Using dedup can cause MemoryLimitExceededException on large datasets, so do the dedup in the set

            GraphTraversal<Edge, String> traversal = g.E().label();

            logger.info(GremlinQueryDebugger.queryAsString(traversal));

            Set<Label> labels = new HashSet<>();
            traversal.forEachRemaining(r -> labels.add(new Label(r)));
            return labels;
        }

        @Override
        public Label getLabelFor(Map<String, Object> input) {
            return new Label(input.get("~label").toString());
        }

        @Override
        public String[] additionalColumns(String... columns) {
            return columns;
        }

        @Override
        public <T> GraphTraversal<? extends Element, T> addAdditionalColumns(GraphTraversal<? extends Element, T> t) {
            return t;
        }

    },
    edgeAndVertexLabels {
        @Override
        public Collection<Label> getLabels(GraphTraversalSource g) {
            // Using dedup can cause MemoryLimitExceededException on large datasets, so do the dedup in the set

            GraphTraversal<Edge, Map<String, Object>> traversal = g.E()
                    .project("~fromLabels", "~label", "~toLabels")
                    .by(outV().label().fold())
                    .by(label())
                    .by(inV().label().fold());

            logger.info(GremlinQueryDebugger.queryAsString(traversal));

            Set<Label> labels = new HashSet<>();
            traversal.forEachRemaining(r -> {
                labels.add(getLabelFor(r));
            });

            return labels;
        }

        @Override
        public Label getLabelFor(Map<String, Object> input) {
            @SuppressWarnings("unchecked")
            Collection<String> fromLabels = (Collection<String>) input.get("~fromLabels");
            String label = String.valueOf(input.get("~label"));
            @SuppressWarnings("unchecked")
            Collection<String> toLabels = (Collection<String>) input.get("~toLabels");
            return new Label(label, fromLabels, toLabels);
        }

        @Override
        public String[] additionalColumns(String... columns) {
            return ArrayUtils.addAll(columns, "~fromLabels", "~toLabels");
        }

        @Override
        public <T> GraphTraversal<? extends Element, T> addAdditionalColumns(GraphTraversal<? extends Element, T> t) {
            return t.by(outV().label().fold()).by(inV().label().fold());
        }

    };

    private static final Logger logger = LoggerFactory.getLogger(EdgeLabelStrategy.class);
}
