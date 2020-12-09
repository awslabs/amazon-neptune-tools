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

package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.TrainingJobWriterConfig;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ParseLabels {

    private final Collection<JsonNode> nodes;
    private final Collection<Double> defaultSplitRates;

    public ParseLabels(Collection<JsonNode> nodes, Collection<Double> defaultSplitRates) {
        this.nodes = nodes;
        this.defaultSplitRates = defaultSplitRates;
    }

    public Map<Label, TrainingJobWriterConfig.LabelConfig> parseNodeClassLabels() {
        Map<Label, TrainingJobWriterConfig.LabelConfig> nodeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            if (isNodeClass(node)) {
                String description = "node label";
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String property = new ParseProperty(node, description).parseSingleProperty();
                String labelType = new ParseLabelType("node", node).parseLabel();
                Collection<Double> splitRates = new ParseSplitRate(node, defaultSplitRates).parseSplitRates();
                nodeClassLabels.put(nodeType, new TrainingJobWriterConfig.LabelConfig(labelType, property, splitRates));
            }
        }
        return nodeClassLabels;
    }

    public Map<Label, TrainingJobWriterConfig.LabelConfig> parseEdgeClassLabels() {
        Map<Label, TrainingJobWriterConfig.LabelConfig> edgeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            if (isEdgeClass(node)) {
                String description = "edge label";
                Label edgeType = new ParseEdgeType(node, description).parseEdgeType();
                String property = new ParseProperty(node, description).parseSingleProperty();
                String labelType = new ParseLabelType("edge", node).parseLabel();
                Collection<Double> splitRates = new ParseSplitRate(node, defaultSplitRates).parseSplitRates();
                edgeClassLabels.put(edgeType, new TrainingJobWriterConfig.LabelConfig(labelType, property, splitRates));
            }
        }
        return edgeClassLabels;
    }

    public void validate() {
        for (JsonNode node : nodes) {
            if (!isNodeClass(node) && !isEdgeClass(node)){
                throw new IllegalArgumentException("Illegal label element: expected 'node' or 'edge' field, and a 'property' field");
            }
        }
    }

    private boolean isNodeClass(JsonNode node) {
        return node.has("node") && node.has("property");
    }

    private boolean isEdgeClass(JsonNode node) {
        return node.has("edge") && node.has("property");
    }
}
