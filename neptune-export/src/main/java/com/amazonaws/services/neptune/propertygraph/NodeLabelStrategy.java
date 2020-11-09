/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public enum NodeLabelStrategy implements LabelStrategy {

    nodeLabelsOnly {
        @Override
        public Collection<Label> getLabels(GraphTraversalSource g) {
            // Using dedup can cause MemoryLimitExceededException on large datasets, so do the dedup in the set

            GraphTraversal<Vertex, String> traversal = g.V().label();

            logger.info(GremlinQueryDebugger.queryAsString(traversal));

            Set<Label> labels = new HashSet<>();
            traversal.forEachRemaining(r -> labels.add(new Label(r)));
            return labels;
        }

        @Override
        public Label getLabelFor(Map<String, Object> input) {
            @SuppressWarnings("unchecked")
            List<String> labels = (List<String>) input.get("~label");
            labels = Label.fixLabelsIssue(labels);

            return new Label(labels);
        }

        @Override
        public String[] additionalColumns(String... columns) {
            return columns;
        }

        @Override
        public <T> GraphTraversal<? extends Element, T> addAdditionalColumns(GraphTraversal<? extends Element, T> t) {
            return t;
        }

    };

    private static final Logger logger = LoggerFactory.getLogger(NodeLabelStrategy.class);
}
