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

package com.amazonaws.services.neptune.profiles.neptune_ml.v1.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.parsing.ParseLabelsV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParseSplitRate;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.parsing.*;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.*;
import java.util.stream.Collectors;

public class TrainingDataWriterConfigV1 {

    public static final Collection<Double> DEFAULT_SPLIT_RATES_V1 = Arrays.asList(0.7, 0.1, 0.2);
    private static final String DEFAULT_NAME_V1 = "training-job-configuration";

    public static Collection<TrainingDataWriterConfigV1> fromJson(JsonNode json) {

        Collection<TrainingDataWriterConfigV1> results = new ArrayList<>();

        if (json.isArray()) {
            ArrayNode configNodes = (ArrayNode) json;
            int index = 1;
            for (JsonNode configNode : configNodes) {
                results.add(getTrainingJobWriterConfig(configNode, index++));
            }
        } else {
            results.add(getTrainingJobWriterConfig(json, 1));
        }

        Set<String> names = results.stream().map(TrainingDataWriterConfigV1::name).collect(Collectors.toSet());

        if (names.size() < results.size()) {
            throw new IllegalStateException(String.format("Training job configuration names must be unique: %s", names));
        }

        return results;
    }

    private static TrainingDataWriterConfigV1 getTrainingJobWriterConfig(JsonNode json, int index) {
        Map<Label, LabelConfigV1> nodeClassLabels = new HashMap<>();
        Map<Label, LabelConfigV1> edgeClassLabels = new HashMap<>();
        Collection<Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
        Collection<NumericalBucketFeatureConfigV1> numericalBucketFeatures = new ArrayList<>();
        Collection<FeatureOverrideConfigV1> nodeFeatureOverrides = new ArrayList<>();
        Collection<FeatureOverrideConfigV1> edgeFeatureOverrides = new ArrayList<>();

        Collection<Double> defaultSplitRates = new ParseSplitRate(json, DEFAULT_SPLIT_RATES_V1, new ParsingContext("config")).parseSplitRates();

        String name = json.has("name") ?
                json.get("name").textValue() :
                index > 1 ? String.format("%s-%s", DEFAULT_NAME_V1, index) : DEFAULT_NAME_V1;

        if (json.has("targets")) {
            JsonNode labels = json.path("targets");
            Collection<JsonNode> labelNodes = new ArrayList<>();
            if (labels.isArray()) {
                labels.forEach(labelNodes::add);
            } else {
                labelNodes.add(labels);
            }
            ParseLabelsV1 parseLabels = new ParseLabelsV1(labelNodes, defaultSplitRates);
            parseLabels.validate();
            nodeClassLabels.putAll(parseLabels.parseNodeClassLabels());
            edgeClassLabels.putAll(parseLabels.parseEdgeClassLabels());
        }

        if (json.has("features")) {
            JsonNode features = json.path("features");
            Collection<JsonNode> featureNodes = new ArrayList<>();
            if (features.isArray()) {
                features.forEach(featureNodes::add);
            } else {
                featureNodes.add(features);
            }
            ParseFeaturesV1 parseFeatures = new ParseFeaturesV1(featureNodes);
            parseFeatures.validate();
            word2VecNodeFeatures.addAll(parseFeatures.parseWord2VecNodeFeatures());
            numericalBucketFeatures.addAll(parseFeatures.parseNumericalBucketFeatures());
            nodeFeatureOverrides.addAll(parseFeatures.parseNodeFeatureOverrides());
            edgeFeatureOverrides.addAll(parseFeatures.parseEdgeFeatureOverrides());
        }

        return new TrainingDataWriterConfigV1(
                name,
                nodeClassLabels,
                edgeClassLabels,
                word2VecNodeFeatures,
                numericalBucketFeatures,
                nodeFeatureOverrides,
                edgeFeatureOverrides);
    }

    private final String name;
    private final Map<Label, LabelConfigV1> nodeClassLabels;
    private final Map<Label, LabelConfigV1> edgeClassLabels;
    private final Collection<Word2VecConfig> word2VecNodeFeatures;
    private final Collection<NumericalBucketFeatureConfigV1> numericalBucketFeatures;
    private final Collection<FeatureOverrideConfigV1> nodeFeatureOverrides;
    private final Collection<FeatureOverrideConfigV1> edgeFeatureOverrides;

    public TrainingDataWriterConfigV1() {
        this(DEFAULT_NAME_V1,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }

    public TrainingDataWriterConfigV1(String name, Map<Label, LabelConfigV1> nodeClassLabels,
                                      Map<Label, LabelConfigV1> edgeClassLabels,
                                      Collection<Word2VecConfig> word2VecNodeFeatures,
                                      Collection<NumericalBucketFeatureConfigV1> numericalBucketFeatures,
                                      Collection<FeatureOverrideConfigV1> nodeFeatureOverrides,
                                      Collection<FeatureOverrideConfigV1> edgeFeatureOverrides) {
        this.name = name;
        this.nodeClassLabels = nodeClassLabels;
        this.edgeClassLabels = edgeClassLabels;
        this.word2VecNodeFeatures = word2VecNodeFeatures;
        this.numericalBucketFeatures = numericalBucketFeatures;
        this.nodeFeatureOverrides = nodeFeatureOverrides;
        this.edgeFeatureOverrides = edgeFeatureOverrides;
    }

    public boolean hasNodeClassificationSpecificationForNode(Label nodeType) {
        return nodeClassLabels.containsKey(nodeType);
    }

    public LabelConfigV1 getNodeClassificationPropertyForNode(Label nodeType) {
        return nodeClassLabels.get(nodeType);
    }

    public boolean isNodeClassificationPropertyForNode(Label nodeType, String property) {
        if (hasNodeClassificationSpecificationForNode(nodeType)) {
            return getNodeClassificationPropertyForNode(nodeType).property().equals(property);
        } else {
            return false;
        }
    }

    public boolean hasEdgeClassificationSpecificationForEdge(Label edgeType) {
        return edgeClassLabels.containsKey(edgeType);
    }

    public LabelConfigV1 getEdgeClassificationPropertyForEdge(Label nodeType) {
        return edgeClassLabels.get(nodeType);
    }

    public boolean isEdgeClassificationPropertyForEdge(Label edgeType, String property) {
        if (hasEdgeClassificationSpecificationForEdge(edgeType)) {
            return getEdgeClassificationPropertyForEdge(edgeType).property().equals(property);
        } else {
            return false;
        }
    }

    public boolean hasWord2VecSpecification(Label nodeType, String property) {
        return getWord2VecSpecification(nodeType, property) != null;
    }

    public Word2VecConfig getWord2VecSpecification(Label nodeType, String property) {
        return word2VecNodeFeatures.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasNumericalBucketSpecification(Label nodeType, String property) {
        return getNumericalBucketSpecification(nodeType, property) != null;
    }

    public NumericalBucketFeatureConfigV1 getNumericalBucketSpecification(Label nodeType, String property) {
        return numericalBucketFeatures.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasNodeFeatureOverrideForNodeProperty(Label nodeType, String property) {
        return nodeFeatureOverrides.stream()
                .anyMatch(override ->
                        override.label().equals(nodeType) &&
                                override.properties().contains(property));
    }

    public Collection<FeatureOverrideConfigV1> getNodeFeatureOverrides(Label nodeType) {
        return nodeFeatureOverrides.stream()
                .filter(c -> c.label().equals(nodeType))
                .collect(Collectors.toList());
    }

    public FeatureOverrideConfigV1 getNodeFeatureOverride(Label nodeType, String property) {
        return nodeFeatureOverrides.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.properties().contains(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasEdgeFeatureOverrideForEdgeProperty(Label edgeType, String property) {
        return edgeFeatureOverrides.stream()
                .anyMatch(override ->
                        override.label().equals(edgeType) &&
                                override.properties().contains(property));
    }

    public Collection<FeatureOverrideConfigV1> getEdgeFeatureOverrides(Label edgeType) {
        return edgeFeatureOverrides.stream()
                .filter(c -> c.label().equals(edgeType))
                .collect(Collectors.toList());
    }

    public FeatureOverrideConfigV1 getEdgeFeatureOverride(Label edgeType, String property) {
        return edgeFeatureOverrides.stream()
                .filter(config ->
                        config.label().equals(edgeType) &&
                                config.properties().contains(property))
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return "TrainingJobWriterConfig{" +
                "nodeClassLabels=" + nodeClassLabels +
                ", edgeClassLabels=" + edgeClassLabels +
                ", word2VecNodeFeatures=" + word2VecNodeFeatures +
                ", numericalBucketFeatures=" + numericalBucketFeatures +
                ", nodeFeatureOverrides=" + nodeFeatureOverrides +
                ", edgeFeatureOverrides=" + edgeFeatureOverrides +
                '}';
    }

    public String name() {
        return name;
    }

}
