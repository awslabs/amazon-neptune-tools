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

package com.amazonaws.services.neptune.propertygraph.metadata;

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

public class MetadataTypes {
    public static final GraphElementType<Map<String, Object>> Nodes = new GraphElementType<Map<String, Object>>() {
        @Override
        public String name() {
            return "nodes";
        }

        @Override
        public GraphClient<Map<String, Object>> graphClient(GraphTraversalSource g, boolean tokensOnly, ExportStats stats, Collection<String> labModeFeatures) {
            return new NodesClient(g, tokensOnly, stats, labModeFeatures);
        }

        @Override
        public WriterFactory<Map<String, Object>> writerFactory() {
            return new NodesWriterFactory();
        }
    };

    public static final GraphElementType<Map<String, Object>> Edges = new GraphElementType<Map<String, Object>>() {
        @Override
        public String name() {
            return "edges";
        }

        @Override
        public GraphClient<Map<String, Object>> graphClient(GraphTraversalSource g, boolean tokensOnly, ExportStats stats, Collection<String> labModeFeatures) {
            return new EdgesClient(g, tokensOnly, stats, labModeFeatures);
        }

        @Override
        public WriterFactory<Map<String, Object>> writerFactory() {
            return new EdgesWriterFactory();
        }
    };

    public static Collection<GraphElementType<?>> values() {
        return Arrays.asList(Nodes, Edges);
    }
}
