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

package com.amazonaws.services.neptune.profiles.neptune_ml.v1.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.v1.TrainingJobWriterConfigV1;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ParseLabelsV1 {

    private final Collection<JsonNode> nodes;
    private final Collection<Double> defaultSplitRates;

    public ParseLabelsV1(Collection<JsonNode> nodes, Collection<Double> defaultSplitRates) {
        this.nodes = nodes;
        this.defaultSplitRates = defaultSplitRates;
    }

    public Map<Label, TrainingJobWriterConfigV1.LabelConfig> parseNodeClassLabels() {
        Map<Label, TrainingJobWriterConfigV1.LabelConfig> nodeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            if (isNodeClass(node)) {
                String description = "node label";
                Label nodeType = new ParseNodeTypeV1(node, description).parseNodeType();
                String property = new ParsePropertyV1(node, description).parseSingleProperty();
                String labelType = new ParseLabelTypeV1("node", node).parseLabel();
                Collection<Double> splitRates = new ParseSplitRateV1(node, defaultSplitRates).parseSplitRates();
                nodeClassLabels.put(nodeType, new TrainingJobWriterConfigV1.LabelConfig(labelType, property, splitRates));
            }
        }
        return nodeClassLabels;
    }

    public Map<Label, TrainingJobWriterConfigV1.LabelConfig> parseEdgeClassLabels() {
        Map<Label, TrainingJobWriterConfigV1.LabelConfig> edgeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            if (isEdgeClass(node)) {
                String description = "edge label";
                Label edgeType = new ParseEdgeTypeV1(node, description).parseEdgeType();
                String property = new ParsePropertyV1(node, description).parseSingleProperty();
                String labelType = new ParseLabelTypeV1("edge", node).parseLabel();
                Collection<Double> splitRates = new ParseSplitRateV1(node, defaultSplitRates).parseSplitRates();
                edgeClassLabels.put(edgeType, new TrainingJobWriterConfigV1.LabelConfig(labelType, property, splitRates));
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
