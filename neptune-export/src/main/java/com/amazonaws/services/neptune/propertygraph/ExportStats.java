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

import com.amazonaws.services.neptune.propertygraph.metadata.MetadataTypes;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadataCollection;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ExportStats {
    private long nodeCount = 0;
    private long edgeCount = 0;

    private final ConcurrentHashMap<String, LabelStats> nodeStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LabelStats> edgeStats = new ConcurrentHashMap<>();

    public void setNodeCount(long value) {
        nodeCount = value;
    }

    public void setEdgeCount(long value) {
        edgeCount = value;
    }


    public void incrementNodeStats(String label) {
        nodeStats.computeIfAbsent(label, LabelStats::new).increment();
    }

    public void incrementEdgeStats(String label) {
        edgeStats.computeIfAbsent(label, LabelStats::new).increment();
    }

    public void prepare(PropertiesMetadataCollection metadataCollection) {
        for (String label : metadataCollection.propertyMetadataFor(MetadataTypes.Nodes).labels()) {
            nodeStats.put(label, new LabelStats(label));
        }
        for (String label : metadataCollection.propertyMetadataFor(MetadataTypes.Edges).labels()) {
            edgeStats.put(label, new LabelStats(label));
        }
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("Source:").append(System.lineSeparator());
        sb.append("  Nodes: ").append(nodeCount).append(System.lineSeparator());
        sb.append("  Edges: ").append(edgeCount).append(System.lineSeparator());
        sb.append("Export:").append(System.lineSeparator());
        sb.append("  Nodes: ").append(nodeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum)).append(System.lineSeparator());
        sb.append("  Edges: ").append(edgeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum)).append(System.lineSeparator());
        sb.append("Details:").append(System.lineSeparator());
        sb.append("  Nodes: ").append(System.lineSeparator());
        for (LabelStats entry : nodeStats.values()) {
            sb.append("    ").append(entry.toString()).append(System.lineSeparator());
        }
        sb.append("  Edges: ").append(System.lineSeparator());
        for (LabelStats entry : edgeStats.values()) {
            sb.append("    ").append(entry.toString()).append(System.lineSeparator());
        }

        return sb.toString();
    }

    public void addTo(ObjectNode exportNode) {
        ObjectNode statsNode = JsonNodeFactory.instance.objectNode();
        exportNode.set("stats", statsNode);
        statsNode.put("nodes", nodeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum));
        statsNode.put("edges", edgeStats.values().stream().map(LabelStats::count).reduce(0L, Long::sum));
        ObjectNode detailsNode = JsonNodeFactory.instance.objectNode();
        statsNode.set("details", detailsNode);
        ArrayNode nodesArrayNode = JsonNodeFactory.instance.arrayNode();
        detailsNode.set("nodes", nodesArrayNode);
        for (LabelStats entry : nodeStats.values()) {
            ObjectNode nodeNode = JsonNodeFactory.instance.objectNode();
            nodesArrayNode.add(nodeNode);
            nodeNode.put("label", entry.label());
            nodeNode.put("count", entry.count());
        }
        ArrayNode edgesArrayNode = JsonNodeFactory.instance.arrayNode();
        detailsNode.set("edges", edgesArrayNode);
        for (LabelStats entry : edgeStats.values()) {
            ObjectNode edgeNode = JsonNodeFactory.instance.objectNode();
            edgesArrayNode.add(edgeNode);
            edgeNode.put("label", entry.label());
            edgeNode.put("count", entry.count());
        }
    }


    private static class LabelStats {
        private final String label;
        private final AtomicLong count = new AtomicLong(0);

        private LabelStats(String label) {
            this.label = label;
        }

        public void increment() {
            count.incrementAndGet();
        }

        public long count() {
            return count.get();
        }

        public String label(){
            return label;
        }

        @Override
        public String toString() {
            return String.format("%s: %s", label, count.get());
        }
    }
}
