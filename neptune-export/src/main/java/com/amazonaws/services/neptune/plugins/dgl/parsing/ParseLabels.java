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

package com.amazonaws.services.neptune.plugins.dgl.parsing;

import com.amazonaws.services.neptune.plugins.dgl.TrainingJobWriterConfig;
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
            String labelType = node.get("label_type").textValue();
            String subLabelType = node.get("sub_label_type").textValue();
            String description = String.format("label_type '%s' and sub_label_type '%s'", labelType, subLabelType);
            if (isNodeClass(labelType, subLabelType)) {
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String col = new ParseCols(node, description).parseSingleColumn();
                Collection<Double> splitRates = new ParseSplitRate(node, defaultSplitRates).parseSplitRates();
                nodeClassLabels.put(nodeType, new TrainingJobWriterConfig.LabelConfig(col, splitRates));
            }
        }
        return nodeClassLabels;
    }

    public Map<Label, TrainingJobWriterConfig.LabelConfig> parseEdgeClassLabels() {
        Map<Label, TrainingJobWriterConfig.LabelConfig> edgeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            String labelType = node.get("label_type").textValue();
            String subLabelType = node.get("sub_label_type").textValue();
            String description = String.format("label_type '%s' and sub_label_type '%s'", labelType, subLabelType);
            if (isEdgeClass(labelType, subLabelType)) {
                Label edgeType = new ParseEdgeType(node, description).parseEdgeType();
                String col = new ParseCols(node, description).parseSingleColumn();
                Collection<Double> splitRates = new ParseSplitRate(node, defaultSplitRates).parseSplitRates();
                edgeClassLabels.put(edgeType, new TrainingJobWriterConfig.LabelConfig(col, splitRates));
            }
        }
        return edgeClassLabels;
    }

    public void validate() {
        for (JsonNode node : nodes) {
            if (node.has("label_type") && node.has("sub_label_type")) {
                String labelType = node.get("label_type").textValue();
                String subLabelType = node.get("sub_label_type").textValue();
                String description = String.format("label_type '%s' and sub_label_type '%s'", labelType, subLabelType);
                if (!isNodeClass(labelType, subLabelType) && !isEdgeClass(labelType, subLabelType)) {
                    throw new IllegalArgumentException(String.format("Unrecognized field values: %s", description));
                }
            } else {
                throw new IllegalArgumentException("Illegal label element: expected 'label_type' and 'sub_label_type' fields");
            }
        }
    }

    private boolean isEdgeClass(String labelType, String subLabelType) {
        return labelType.equals("edge") && subLabelType.equals("edge_class_label");
    }

    private boolean isNodeClass(String labelType, String subLabelType) {
        return labelType.equals("node") && subLabelType.equals("node_class_label");
    }
}
