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

package com.amazonaws.services.neptune.propertygraph.schema;

import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.propertygraph.EdgesClient;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.GraphClient;
import com.amazonaws.services.neptune.propertygraph.NodesClient;
import com.amazonaws.services.neptune.propertygraph.io.EdgesWriterFactory;
import com.amazonaws.services.neptune.propertygraph.io.NodesWriterFactory;
import com.amazonaws.services.neptune.propertygraph.io.WriterFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public enum GraphElementType {

    nodes {
        @Override
        public Collection<String> tokenNames() {
            return Arrays.asList("~id", "~label");
        }

        @Override
        public GraphClient<Map<String, Object>> graphClient(GraphTraversalSource g, boolean tokensOnly, ExportStats stats, FeatureToggles featureToggles) {
            return new NodesClient(g, tokensOnly, stats, featureToggles);
        }

        @Override
        public WriterFactory<Map<String, Object>> writerFactory() {
            return new NodesWriterFactory();
        }
    },
    edges {
        @Override
        public Collection<String> tokenNames() {
            return Arrays.asList("~id", "~label", "~from", "~to");
        }

        @Override
        public GraphClient<Map<String, Object>> graphClient(GraphTraversalSource g, boolean tokensOnly, ExportStats stats, FeatureToggles featureToggles) {
            return new EdgesClient(g, tokensOnly, stats, featureToggles);
        }

        @Override
        public WriterFactory<Map<String, Object>> writerFactory() {
            return new EdgesWriterFactory();
        }
    };

    public abstract Collection<String> tokenNames();
    public abstract GraphClient<Map<String, Object>> graphClient(GraphTraversalSource g, boolean tokensOnly, ExportStats stats, FeatureToggles featureToggles);
    public abstract WriterFactory<Map<String, Object>> writerFactory();
}
