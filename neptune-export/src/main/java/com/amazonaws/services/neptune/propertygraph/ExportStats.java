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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ExportStats {
    private long nodeCount = 0;
    private long edgeCount = 0;

    private final Map<String, LabelStats> nodeStats = new HashMap<>();
    private final Map<String, LabelStats> edgeStats = new HashMap<>();

    public void setNodeCount(long value) {
        nodeCount = value;
    }

    public void setEdgeCount(long value) {
        edgeCount = value;
    }


    public void incrementNodeStats(String label) {
        nodeStats.get(label).increment();
    }

    public void incrementEdgeStats(String label) {
        edgeStats.get(label).increment();
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

        @Override
        public String toString() {
            return String.format("%s: %s", label, count.get());
        }
    }
}
