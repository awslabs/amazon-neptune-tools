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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.LabelConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParseLabels;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParseSplitRate;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing.ParseFeaturesV2;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.*;
import java.util.stream.Collectors;

public class TrainingDataWriterConfigV2 {

    public static final Collection<Double> DEFAULT_SPLIT_RATES_V2 = Arrays.asList(0.8, 0.2, 0.0);
    private static final String DEFAULT_NAME_V2 = "training-data-configuration";

    public static Collection<TrainingDataWriterConfigV2> fromJson(JsonNode json) {

        Collection<TrainingDataWriterConfigV2> results = new ArrayList<>();

        if (json.isArray()) {
            ArrayNode configNodes = (ArrayNode) json;
            int index = 1;
            for (JsonNode configNode : configNodes) {
                results.add(getTrainingJobWriterConfig(configNode, index++));
            }
        } else {
            results.add(getTrainingJobWriterConfig(json, 1));
        }

        Set<String> names = results.stream().map(TrainingDataWriterConfigV2::name).collect(Collectors.toSet());

        if (names.size() < results.size()) {
            throw new IllegalStateException(String.format("Training data configuration names must be unique: %s", names));
        }

        return results;
    }

    private static TrainingDataWriterConfigV2 getTrainingJobWriterConfig(JsonNode json, int index) {

        Collection<Double> defaultSplitRates = new ParseSplitRate(json, DEFAULT_SPLIT_RATES_V2, new ParsingContext("config")).parseSplitRates();
        Map<Label, LabelConfig> nodeClassLabels = new HashMap<>();
        Map<Label, LabelConfig> edgeClassLabels = new HashMap<>();
        Collection<TfIdfConfigV2> tfIdfNodeFeatures = new ArrayList<>();
        Collection<DatetimeConfigV2> datetimeNodeFeatures = new ArrayList<>();
        Collection<Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
        Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures = new ArrayList<>();
        Collection<FeatureOverrideConfigV2> nodeFeatureOverrides = new ArrayList<>();
        Collection<FeatureOverrideConfigV2> edgeFeatureOverrides = new ArrayList<>();

        String name = json.has("name") ?
                json.get("name").textValue() :
                index > 1 ? String.format("%s-%s", DEFAULT_NAME_V2, index) : DEFAULT_NAME_V2;

        if (json.has("targets")) {
            JsonNode labels = json.path("targets");
            Collection<JsonNode> labelNodes = new ArrayList<>();
            if (labels.isArray()) {
                labels.forEach(labelNodes::add);
            } else {
                labelNodes.add(labels);
            }
            ParseLabels parseLabels = new ParseLabels(labelNodes, defaultSplitRates);
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

            ParseFeaturesV2 parseFeatures = new ParseFeaturesV2(featureNodes);

            parseFeatures.validate();

            tfIdfNodeFeatures.addAll(parseFeatures.parseTfIdfNodeFeatures());
            datetimeNodeFeatures.addAll(parseFeatures.parseDatetimeFeatures());
            word2VecNodeFeatures.addAll(parseFeatures.parseWord2VecNodeFeatures());
            numericalBucketFeatures.addAll(parseFeatures.parseNumericalBucketFeatures());
            nodeFeatureOverrides.addAll(parseFeatures.parseNodeFeatureOverrides());
            edgeFeatureOverrides.addAll(parseFeatures.parseEdgeFeatureOverrides());

        }


        return new TrainingDataWriterConfigV2(name,
                nodeClassLabels,
                edgeClassLabels,
                tfIdfNodeFeatures,
                datetimeNodeFeatures,
                word2VecNodeFeatures,
                numericalBucketFeatures,
                nodeFeatureOverrides,
                edgeFeatureOverrides);
    }

    private final String name;
    private final Map<Label, LabelConfig> nodeClassLabels;
    private final Map<Label, LabelConfig> edgeClassLabels;
    private final Collection<TfIdfConfigV2> tfIdfNodeFeatures;
    private final Collection<DatetimeConfigV2> datetimeNodeFeatures;
    private final Collection<Word2VecConfig> word2VecNodeFeatures;
    private final Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures;
    private final Collection<FeatureOverrideConfigV2> nodeFeatureOverrides;
    private final Collection<FeatureOverrideConfigV2> edgeFeatureOverrides;

    public TrainingDataWriterConfigV2() {
        this(DEFAULT_NAME_V2,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }

    public TrainingDataWriterConfigV2(String name,
                                      Map<Label, LabelConfig> nodeClassLabels,
                                      Map<Label, LabelConfig> edgeClassLabels,
                                      Collection<TfIdfConfigV2> tfIdfNodeFeatures,
                                      Collection<DatetimeConfigV2> datetimeNodeFeatures,
                                      Collection<Word2VecConfig> word2VecNodeFeatures,
                                      Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures,
                                      Collection<FeatureOverrideConfigV2> nodeFeatureOverrides,
                                      Collection<FeatureOverrideConfigV2> edgeFeatureOverrides) {
        this.name = name;
        this.nodeClassLabels = nodeClassLabels;
        this.edgeClassLabels = edgeClassLabels;
        this.tfIdfNodeFeatures = tfIdfNodeFeatures;
        this.datetimeNodeFeatures = datetimeNodeFeatures;
        this.word2VecNodeFeatures = word2VecNodeFeatures;
        this.numericalBucketFeatures = numericalBucketFeatures;
        this.nodeFeatureOverrides = nodeFeatureOverrides;
        this.edgeFeatureOverrides = edgeFeatureOverrides;
    }

    public boolean allowAutoInferNodeFeature(Label nodeType, String property){
        if (isNodeClassificationPropertyForNode(nodeType, property)) {
            return false;
        }
        if (hasTfIdfSpecification(nodeType, property)){
            return false;
        }
        if (hasDatetimeSpecification(nodeType, property)){
            return false;
        }
        if (hasWord2VecSpecification(nodeType, property)){
            return false;
        }
        if (hasNumericalBucketSpecification(nodeType, property)){
            return false;
        }
        if (hasNodeFeatureOverrideForNodeProperty(nodeType, property)){
            return false;
        }
        return true;
    }

    public boolean allowAutoInferEdgeFeature(Label edgeType, String property){
        if (isEdgeClassificationPropertyForEdge(edgeType, property)) {
            return false;
        }
        if (hasEdgeFeatureOverrideForEdgeProperty(edgeType, property)){
            return false;
        }
        return true;
    }

    public boolean hasNodeClassificationSpecificationForNode(Label nodeType) {
        return nodeClassLabels.containsKey(nodeType);
    }

    public LabelConfig getNodeClassificationPropertyForNode(Label nodeType) {
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

    public LabelConfig getEdgeClassificationPropertyForEdge(Label nodeType) {
        return edgeClassLabels.get(nodeType);
    }

    public boolean isEdgeClassificationPropertyForEdge(Label edgeType, String property) {
        if (hasEdgeClassificationSpecificationForEdge(edgeType)) {
            return getEdgeClassificationPropertyForEdge(edgeType).property().equals(property);
        } else {
            return false;
        }
    }

    public boolean hasTfIdfSpecification(Label nodeType, String property) {
        return getTfIdfSpecification(nodeType, property) != null;
    }

    public TfIdfConfigV2 getTfIdfSpecification(Label nodeType, String property) {
        return tfIdfNodeFeatures.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasDatetimeSpecification(Label nodeType, String property) {
        return getDatetimeSpecification(nodeType, property) != null;
    }

    public DatetimeConfigV2 getDatetimeSpecification(Label nodeType, String property) {
        return datetimeNodeFeatures.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
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

    public NumericalBucketFeatureConfigV2 getNumericalBucketSpecification(Label nodeType, String property) {
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

    public Collection<FeatureOverrideConfigV2> getNodeFeatureOverrides(Label nodeType) {
        return nodeFeatureOverrides.stream()
                .filter(c -> c.label().equals(nodeType))
                .collect(Collectors.toList());
    }

    public FeatureOverrideConfigV2 getNodeFeatureOverride(Label nodeType, String property) {
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

    public Collection<FeatureOverrideConfigV2> getEdgeFeatureOverrides(Label edgeType) {
        return edgeFeatureOverrides.stream()
                .filter(c -> c.label().equals(edgeType))
                .collect(Collectors.toList());
    }

    public FeatureOverrideConfigV2 getEdgeFeatureOverride(Label edgeType, String property) {
        return edgeFeatureOverrides.stream()
                .filter(config ->
                        config.label().equals(edgeType) &&
                                config.properties().contains(property))
                .findFirst()
                .orElse(null);
    }

    public String name() {
        return name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "TrainingJobWriterConfig{" +
                "nodeClassLabels=" + nodeClassLabels +
                ", edgeClassLabels=" + edgeClassLabels +
                ", tfIdfNodeFeatures=" + tfIdfNodeFeatures +
                ", datetimeNodeFeatures=" + datetimeNodeFeatures +
                ", word2VecNodeFeatures=" + word2VecNodeFeatures +
                ", numericalBucketFeatures=" + numericalBucketFeatures +
                ", nodeFeatureOverrides=" + nodeFeatureOverrides +
                ", edgeFeatureOverrides=" + edgeFeatureOverrides +
                '}';
    }

}
