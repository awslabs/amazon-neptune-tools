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

import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class
ExportStats {
    private long nodeCount = 0;
    private long edgeCount = 0;

    private final ConcurrentHashMap<Label, LabelStats> nodeStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Label, LabelStats> edgeStats = new ConcurrentHashMap<>();

    public void setNodeCount(long value) {
        nodeCount = value;
    }

    public void setEdgeCount(long value) {
        edgeCount = value;
    }


    public void incrementNodeStats(Label label) {
        nodeStats.computeIfAbsent(label, LabelStats::new).increment();
    }

    public void incrementEdgeStats(Label label) {
        edgeStats.computeIfAbsent(label, LabelStats::new).increment();
    }

    public String formatStats(GraphSchema graphSchema) {
        StringBuilder sb = new StringBuilder();

        sb.append("Source:").append(System.lineSeparator());
        sb.append("  Nodes: ").append(nodeCount).append(System.lineSeparator());
        sb.append("  Edges: ").append(edgeCount).append(System.lineSeparator());
        sb.append("Export:").append(System.lineSeparator());
        sb.append("  Nodes: ").append(nodeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum)).append(System.lineSeparator());
        sb.append("  Edges: ").append(edgeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum)).append(System.lineSeparator());

        sb.append("  Properties: ").append(getNumberOfProperties(graphSchema)).append(System.lineSeparator());

        sb.append("Details:").append(System.lineSeparator());

        sb.append("  Nodes: ").append(System.lineSeparator());
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);
        for (Map.Entry<Label, LabelStats> entry : nodeStats.entrySet()) {
            Label label = entry.getKey();
            LabelStats labelStats = entry.getValue();
            LabelSchema labelSchema = nodeSchemas.getSchemaFor(label);
            sb.append("    ").append(labelStats.toString()).append(System.lineSeparator());
            for (PropertySchemaStats stats : labelSchema.propertySchemaStats()) {
                sb.append("        ").append(stats.property()).append(": ").append(stats.observationCount()).append(System.lineSeparator());
            }
        }

        sb.append("  Edges: ").append(System.lineSeparator());
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.edges);
        for (Map.Entry<Label, LabelStats> entry : edgeStats.entrySet()) {
            Label label = entry.getKey();
            LabelStats labelStats = entry.getValue();
            LabelSchema labelSchema = edgeSchemas.getSchemaFor(label);
            sb.append("    ").append(labelStats.toString()).append(System.lineSeparator());
            for (PropertySchemaStats stats : labelSchema.propertySchemaStats()) {
                sb.append("        ").append(stats.property()).append(": ").append(stats.observationCount()).append(System.lineSeparator());
            }
        }

        return sb.toString();
    }

    private Long getNumberOfProperties(GraphSchema graphSchema) {
        return graphSchema.graphElementSchemas().stream()
                .map(s -> s.labelSchemas().stream()
                        .map(l -> l.propertySchemaStats().stream()
                                .map(p -> (long) p.observationCount()).reduce(0L, Long::sum))
                        .reduce(0L, Long::sum))
                .reduce(0L, Long::sum);
    }

    public void addTo(ObjectNode exportNode, GraphSchema graphSchema) {
        ObjectNode statsNode = JsonNodeFactory.instance.objectNode();
        exportNode.set("stats", statsNode);
        statsNode.put("nodes", nodeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum));
        statsNode.put("edges", edgeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum));
        statsNode.put("properties", getNumberOfProperties(graphSchema));
        ObjectNode detailsNode = JsonNodeFactory.instance.objectNode();

        statsNode.set("details", detailsNode);

        ArrayNode nodesArrayNode = JsonNodeFactory.instance.arrayNode();
        detailsNode.set("nodes", nodesArrayNode);

        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);
        for (Map.Entry<Label, LabelStats> entry : nodeStats.entrySet()) {

            Label label = entry.getKey();
            LabelStats labelStats = entry.getValue();
            LabelSchema labelSchema = nodeSchemas.getSchemaFor(label);

            ObjectNode nodeNode = JsonNodeFactory.instance.objectNode();
            nodesArrayNode.add(nodeNode);
            nodeNode.put("label", label.fullyQualifiedLabel());
            nodeNode.put("count", labelStats.count());

            ArrayNode propertiesArray = JsonNodeFactory.instance.arrayNode();

            for (PropertySchemaStats stats : labelSchema.propertySchemaStats()) {
                ObjectNode propertyNode = JsonNodeFactory.instance.objectNode();
                propertyNode.put("name", stats.property().toString());
                propertyNode.put("count", stats.observationCount());
                propertiesArray.add(propertyNode);
            }

            nodeNode.set("properties", propertiesArray);
        }

        ArrayNode edgesArrayNode = JsonNodeFactory.instance.arrayNode();
        detailsNode.set("edges", edgesArrayNode);

        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.edges);
        for (Map.Entry<Label, LabelStats> entry : edgeStats.entrySet()) {
            Label label = entry.getKey();
            LabelStats labelStats = entry.getValue();
            LabelSchema labelSchema = edgeSchemas.getSchemaFor(label);

            ObjectNode edgeNode = JsonNodeFactory.instance.objectNode();
            edgesArrayNode.add(edgeNode);
            edgeNode.put("label", label.fullyQualifiedLabel());
            edgeNode.put("count", labelStats.count());

            ArrayNode propertiesArray = JsonNodeFactory.instance.arrayNode();

            for (PropertySchemaStats stats : labelSchema.propertySchemaStats()) {
                ObjectNode propertyNode = JsonNodeFactory.instance.objectNode();
                propertyNode.put("name", stats.property().toString());
                propertyNode.put("count", stats.observationCount());
                propertiesArray.add(propertyNode);
            }

            edgeNode.set("properties", propertiesArray);
        }
    }


    private static class LabelStats {
        private final Label label;
        private final AtomicLong count = new AtomicLong(0);

        private LabelStats(Label label) {
            this.label = label;
        }

        public void increment() {
            count.incrementAndGet();
        }

        public long count() {
            return count.get();
        }

        public Label label() {
            return label;
        }

        @Override
        public String toString() {
            return String.format("%s: %s", label.fullyQualifiedLabel(), count.get());
        }
    }
}
