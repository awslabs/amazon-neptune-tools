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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.*;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.EdgeTaskTypeV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.LabelConfigV2;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class ParseLabelsV2 {

    private final Collection<JsonNode> config;
    private final Collection<Double> defaultSplitRates;
    private final NeptuneMLSourceDataModel dataModel;

    public ParseLabelsV2(Collection<JsonNode> config, Collection<Double> defaultSplitRates, NeptuneMLSourceDataModel dataModel) {
        this.config = config;
        this.defaultSplitRates = defaultSplitRates;
        this.dataModel = dataModel;
    }

    public Collection<LabelConfigV2> parseNodeClassLabels() {
        Collection<LabelConfigV2> nodeClassLabels = new ArrayList<>();

        for (JsonNode json : config) {
            if (isNodeClass(json)) {
                ParsingContext context = new ParsingContext(String.format("node %s", dataModel.nodeTypeName().toLowerCase()));
                Label nodeType = new ParseNodeType(json, context).parseNodeType();
                String property = dataModel.parseProperty(json, context, nodeType);
                ParsingContext propertyContext = context.withLabel(nodeType).withProperty(property);
                String taskType = dataModel.parseTaskType(json, propertyContext, nodeType, property);
                Separator separator = new ParseSeparator(json).parseSeparator();
                Collection<Double> splitRates = new ParseSplitRate(json, defaultSplitRates, propertyContext).parseSplitRates();
                nodeClassLabels.add(new LabelConfigV2(nodeType, taskType, property, splitRates, separator));
            }
        }

        return nodeClassLabels;
    }

    private boolean isNodeClass(JsonNode json) {
        return json.has("node");
    }

    private boolean isEdgeClass(JsonNode json) {
        return json.has("edge");
    }

    public void validate() {
        for (JsonNode json : config) {
            if (!isNodeClass(json) && !isEdgeClass(json)) {
                throw new IllegalArgumentException("Illegal label element. Expected 'node' or 'edge' field.");
            }
        }
    }

    public Collection<LabelConfigV2> parseEdgeClassLabels() {
        Collection<LabelConfigV2> edgeClassLabels = new ArrayList<>();
        for (JsonNode json : config) {
            if (isEdgeClass(json)) {
                ParsingContext context = new ParsingContext("edge label");
                Label edgeType = new ParseEdgeType(json, context).parseEdgeType();
                String property = new ParseProperty(json, context.withLabel(edgeType)).parseNullableSingleProperty();
                ParsingContext propertyContext = context.withLabel(edgeType).withProperty(property);
                EdgeTaskTypeV2 taskType = new ParseEdgeTaskTypeV2(json, propertyContext).parseTaskType();
                taskType.validate(property, edgeType);
                Separator separator = new ParseSeparator(json).parseSeparator();
                Collection<Double> splitRates = new ParseSplitRate(json, defaultSplitRates, propertyContext).parseSplitRates();
                edgeClassLabels.add(new LabelConfigV2(edgeType, taskType.name(), property, splitRates, separator));
            }
        }
        return edgeClassLabels;
    }
}
