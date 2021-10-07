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

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParseSplitRate;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing.ParseFeaturesV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing.ParseLabelsV2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class TrainingDataWriterConfigV2 {

    public static final Collection<Double> DEFAULT_SPLIT_RATES_V2 = Arrays.asList(0.9, 0.1, 0.0);
    private static final String DEFAULT_NAME_V2 = "training-data-configuration";

    public static Collection<TrainingDataWriterConfigV2> fromJson(JsonNode json, NeptuneMLSourceDataModel dataModel) {

        Collection<TrainingDataWriterConfigV2> results = new ArrayList<>();

        if (json.isArray()) {
            ArrayNode configNodes = (ArrayNode) json;
            int index = 1;
            for (JsonNode configNode : configNodes) {
                results.add(getTrainingJobWriterConfig(configNode, index++, dataModel));
            }
        } else {
            if (json.has("jobs")) {
                ArrayNode configNodes = (ArrayNode) json.get("jobs");
                int index = 1;
                for (JsonNode configNode : configNodes) {
                    results.add(getTrainingJobWriterConfig(configNode, index++, dataModel));
                }
            } else {
                results.add(getTrainingJobWriterConfig(json, 1, dataModel));
            }
        }

        Set<String> names = results.stream().map(TrainingDataWriterConfigV2::name).collect(Collectors.toSet());

        if (names.size() < results.size()) {
            throw new IllegalStateException(String.format("Training data configuration names must be unique: %s", names));
        }

        return results;
    }

    private static TrainingDataWriterConfigV2 getTrainingJobWriterConfig(JsonNode json, int index, NeptuneMLSourceDataModel dataModel) {

        Collection<Double> defaultSplitRates = new ParseSplitRate(json, DEFAULT_SPLIT_RATES_V2, new ParsingContext("config")).parseSplitRates();
        Collection<LabelConfigV2> nodeClassLabels = new ArrayList<>();
        Collection<LabelConfigV2> edgeClassLabels = new ArrayList<>();
        Collection<NoneFeatureConfig> noneNodeFeatures = new ArrayList<>();
        Collection<TfIdfConfigV2> tfIdfNodeFeatures = new ArrayList<>();
        Collection<DatetimeConfigV2> datetimeNodeFeatures = new ArrayList<>();
        Collection<Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
        Collection<NumericalBucketFeatureConfigV2> numericalBucketNodeFeatures = new ArrayList<>();
        Collection<NoneFeatureConfig> noneEdgeFeatures = new ArrayList<>();
        Collection<TfIdfConfigV2> tfIdfEdgeFeatures = new ArrayList<>();
        Collection<DatetimeConfigV2> datetimeEdgeFeatures = new ArrayList<>();
        Collection<Word2VecConfig> word2VecEdgeFeatures = new ArrayList<>();
        Collection<NumericalBucketFeatureConfigV2> numericalBucketEdgeFeatures = new ArrayList<>();
        Collection<FeatureOverrideConfigV2> nodeFeatureOverrides = new ArrayList<>();
        Collection<FeatureOverrideConfigV2> edgeFeatureOverrides = new ArrayList<>();

        String name = json.has("name") ?
                json.get("name").textValue() :
                index > 1 ? String.format("%s-%s", DEFAULT_NAME_V2, index) : DEFAULT_NAME_V2;

        FeatureEncodingFlag featureEncodingFlag = FeatureEncodingFlag.auto;

        if (json.has("feature_encoding")) {
            try {
                featureEncodingFlag = FeatureEncodingFlag.valueOf(json.path("feature_encoding").textValue());
            } catch (IllegalArgumentException e) {
                // Use default value of auto
            }
        }

        if (json.has("targets")) {
            JsonNode labels = json.path("targets");
            Collection<JsonNode> labelNodes = new ArrayList<>();
            if (labels.isArray()) {
                labels.forEach(labelNodes::add);
            } else {
                labelNodes.add(labels);
            }
            ParseLabelsV2 parseLabels = new ParseLabelsV2(labelNodes, defaultSplitRates, dataModel);
            parseLabels.validate();
            nodeClassLabels.addAll(parseLabels.parseNodeClassLabels());
            edgeClassLabels.addAll(parseLabels.parseEdgeClassLabels());
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

            noneNodeFeatures.addAll(parseFeatures.parseNoneFeatures(ParseFeaturesV2.NodeFeatureFilter, ParseFeaturesV2.NodeLabelSupplier));
            tfIdfNodeFeatures.addAll(parseFeatures.parseTfIdfFeatures(ParseFeaturesV2.NodeFeatureFilter, ParseFeaturesV2.NodeLabelSupplier));
            datetimeNodeFeatures.addAll(parseFeatures.parseDatetimeFeatures(ParseFeaturesV2.NodeFeatureFilter, ParseFeaturesV2.NodeLabelSupplier));
            word2VecNodeFeatures.addAll(parseFeatures.parseWord2VecFeatures(ParseFeaturesV2.NodeFeatureFilter, ParseFeaturesV2.NodeLabelSupplier));
            numericalBucketNodeFeatures.addAll(parseFeatures.parseNumericalBucketFeatures(ParseFeaturesV2.NodeFeatureFilter, ParseFeaturesV2.NodeLabelSupplier));

            noneEdgeFeatures.addAll(parseFeatures.parseNoneFeatures(ParseFeaturesV2.EdgeFeatureFilter, ParseFeaturesV2.EdgeLabelSupplier));
            tfIdfEdgeFeatures.addAll(parseFeatures.parseTfIdfFeatures(ParseFeaturesV2.EdgeFeatureFilter, ParseFeaturesV2.EdgeLabelSupplier));
            datetimeEdgeFeatures.addAll(parseFeatures.parseDatetimeFeatures(ParseFeaturesV2.EdgeFeatureFilter, ParseFeaturesV2.EdgeLabelSupplier));
            word2VecEdgeFeatures.addAll(parseFeatures.parseWord2VecFeatures(ParseFeaturesV2.EdgeFeatureFilter, ParseFeaturesV2.EdgeLabelSupplier));
            numericalBucketEdgeFeatures.addAll(parseFeatures.parseNumericalBucketFeatures(ParseFeaturesV2.EdgeFeatureFilter, ParseFeaturesV2.EdgeLabelSupplier));

            nodeFeatureOverrides.addAll(parseFeatures.parseNodeFeatureOverrides());
            edgeFeatureOverrides.addAll(parseFeatures.parseEdgeFeatureOverrides());

        }

        ElementConfig nodeConfig = new ElementConfig(nodeClassLabels, noneNodeFeatures, tfIdfNodeFeatures, datetimeNodeFeatures, word2VecNodeFeatures, numericalBucketNodeFeatures, nodeFeatureOverrides);
        ElementConfig edgeConfig = new ElementConfig(edgeClassLabels, noneEdgeFeatures, tfIdfEdgeFeatures, datetimeEdgeFeatures, word2VecEdgeFeatures, numericalBucketEdgeFeatures, edgeFeatureOverrides);

        return new TrainingDataWriterConfigV2(name,
                featureEncodingFlag,
                defaultSplitRates,
                nodeConfig,
                edgeConfig);
    }

    private final String name;
    private final FeatureEncodingFlag featureEncodingFlag;
    private final Collection<Double> defaultSplitRates;
    private final ElementConfig nodeConfig;
    private final ElementConfig edgeConfig;

    public TrainingDataWriterConfigV2() {
        this(DEFAULT_NAME_V2, FeatureEncodingFlag.auto, DEFAULT_SPLIT_RATES_V2, ElementConfig.EMPTY_CONFIG, ElementConfig.EMPTY_CONFIG);
    }

    public TrainingDataWriterConfigV2(String name, FeatureEncodingFlag featureEncodingFlag, Collection<Double> defaultSplitRates, ElementConfig nodeConfig, ElementConfig edgeConfig) {
        this.name = name;
        this.featureEncodingFlag = featureEncodingFlag;
        this.defaultSplitRates = defaultSplitRates;
        this.nodeConfig = nodeConfig;
        this.edgeConfig = edgeConfig;
    }

    public String name() {
        return name;
    }

    public boolean allowFeatureEncoding() {
        return featureEncodingFlag == FeatureEncodingFlag.auto;
    }

    public Collection<Double> defaultSplitRates() {
        return defaultSplitRates;
    }

    public ElementConfig nodeConfig() {
        return nodeConfig;
    }

    public ElementConfig edgeConfig() {
        return edgeConfig;
    }

    @Override
    public String toString() {
        return "TrainingDataWriterConfigV2{" +
                "name='" + name + '\'' +
                ", defaultSplitRates=" + defaultSplitRates +
                ", nodeConfig=" + nodeConfig +
                ", edgeConfig=" + edgeConfig +
                '}';
    }
}
