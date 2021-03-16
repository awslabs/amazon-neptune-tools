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

package com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.LabelConfig;
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

    public Map<Label, LabelConfig> parseNodeClassLabels() {
        Map<Label, LabelConfig> nodeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            if (isNodeClass(node)) {
                ParsingContext context = new ParsingContext("node label");
                Label nodeType = new ParseNodeType(node, context).parseNodeType();
                String property = new ParseProperty(node, context.withLabel(nodeType)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(nodeType).withProperty(property);
                String labelType = new ParseLabelType("node", node, propertyContext).parseLabel();
                Collection<Double> splitRates = new ParseSplitRate(node, defaultSplitRates, propertyContext).parseSplitRates();
                nodeClassLabels.put(nodeType, new LabelConfig(labelType, property, splitRates));
            }
        }
        return nodeClassLabels;
    }

    public Map<Label, LabelConfig> parseEdgeClassLabels() {
        Map<Label, LabelConfig> edgeClassLabels = new HashMap<>();
        for (JsonNode node : nodes) {
            if (isEdgeClass(node)) {
                ParsingContext context = new ParsingContext("edge label");
                Label edgeType = new ParseEdgeType(node, context).parseEdgeType();
                String property = new ParseProperty(node, context.withLabel(edgeType)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(edgeType).withProperty(property);
                String labelType = new ParseLabelType("edge", node, propertyContext).parseLabel();
                Collection<Double> splitRates = new ParseSplitRate(node, defaultSplitRates, propertyContext).parseSplitRates();
                edgeClassLabels.put(edgeType, new LabelConfig(labelType, property, splitRates));
            }
        }
        return edgeClassLabels;
    }

    public void validate() {
        for (JsonNode node : nodes) {
            if (!isNodeClass(node) && !isEdgeClass(node)) {
                throw new IllegalArgumentException("Illegal label element. Expected 'node' or 'edge' field, and a 'property' field.");
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
