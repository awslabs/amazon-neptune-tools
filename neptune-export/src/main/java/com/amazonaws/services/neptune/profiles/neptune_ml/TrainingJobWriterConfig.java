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

package com.amazonaws.services.neptune.profiles.neptune_ml;

import com.amazonaws.services.neptune.profiles.neptune_ml.parsing.Norm;
import com.amazonaws.services.neptune.profiles.neptune_ml.parsing.ParseFeatures;
import com.amazonaws.services.neptune.profiles.neptune_ml.parsing.ParseLabels;
import com.amazonaws.services.neptune.profiles.neptune_ml.parsing.ParseSplitRate;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

public class TrainingJobWriterConfig {

    public static final Collection<Double> DEFAULT_SPLIT_RATES = Arrays.asList(0.7, 0.1, 0.2);

    public static TrainingJobWriterConfig fromJson(JsonNode json) {

        Map<Label, LabelConfig> nodeClassLabels = new HashMap<>();
        Map<Label, LabelConfig> edgeClassLabels = new HashMap<>();
        Collection<Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
        Collection<NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
        Collection<FeatureOverrideConfig> nodeFeatureOverrides = new ArrayList<>();
        Collection<FeatureOverrideConfig> edgeFeatureOverrides = new ArrayList<>();

        Collection<Double> defaultSplitRates = new ParseSplitRate(json, DEFAULT_SPLIT_RATES).parseSplitRates();

        if (json.has("labels")) {
            JsonNode labels = json.path("labels");
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
            ParseFeatures parseFeatures = new ParseFeatures(featureNodes);
            parseFeatures.validate();
            word2VecNodeFeatures.addAll(parseFeatures.parseWord2VecNodeFeatures());
            numericalBucketFeatures.addAll(parseFeatures.parseNumericalBucketFeatures());
            nodeFeatureOverrides.addAll(parseFeatures.parseNodeFeatureOverrides());
            edgeFeatureOverrides.addAll(parseFeatures.parseEdgeFeatureOverrides());
        }

        return new TrainingJobWriterConfig(
                nodeClassLabels,
                edgeClassLabels,
                word2VecNodeFeatures,
                numericalBucketFeatures,
                nodeFeatureOverrides,
                edgeFeatureOverrides);
    }

    private final Map<Label, LabelConfig> nodeClassLabels;
    private final Map<Label, LabelConfig> edgeClassLabels;
    private final Collection<Word2VecConfig> word2VecNodeFeatures;
    private final Collection<NumericalBucketFeatureConfig> numericalBucketFeatures;
    private final Collection<FeatureOverrideConfig> nodeFeatureOverrides;
    private final Collection<FeatureOverrideConfig> edgeFeatureOverrides;

    public TrainingJobWriterConfig() {
        this(Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
    }

    public TrainingJobWriterConfig(Map<Label, LabelConfig> nodeClassLabels,
                                   Map<Label, LabelConfig> edgeClassLabels,
                                   Collection<Word2VecConfig> word2VecNodeFeatures,
                                   Collection<NumericalBucketFeatureConfig> numericalBucketFeatures,
                                   Collection<FeatureOverrideConfig> nodeFeatureOverrides,
                                   Collection<FeatureOverrideConfig> edgeFeatureOverrides) {
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

    public NumericalBucketFeatureConfig getNumericalBucketSpecification(Label nodeType, String property) {
        return numericalBucketFeatures.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasNodeFeatureOverrideForNodeProperty(Label nodeType, String property){
        return getNodeFeatureOverride(nodeType, property) != null;
    }

    public FeatureOverrideConfig getNodeFeatureOverride(Label nodeType, String property){
        return nodeFeatureOverrides.stream()
                .filter(config ->
                        config.label().equals(nodeType) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasEdgeFeatureOverrideForEdgeProperty(Label edgeType, String property){
        return getEdgeFeatureOverride(edgeType, property) != null;
    }

    public FeatureOverrideConfig getEdgeFeatureOverride(Label edgeType, String property){
        return edgeFeatureOverrides.stream()
                .filter(config ->
                        config.label().equals(edgeType) &&
                                config.property().equals(property))
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

    public static class Word2VecConfig {
        private final Label label;
        private final String property;
        private final Collection<String> languages;

        public Word2VecConfig(Label label, String property, Collection<String> languages) {
            this.label = label;
            this.property = property;
            this.languages = languages;
        }

        public Label label() {
            return label;
        }

        public String property() {
            return property;
        }

        public Collection<String> languages() {
            return languages;
        }

        @Override
        public String toString() {
            return "Word2VecConfig{" +
                    "label=" + label +
                    ", property='" + property + '\'' +
                    ", languages=" + languages +
                    '}';
        }
    }

    public static class LabelConfig {
        private final String labelType;
        private final String property;
        private final Collection<Double> splitRates;

        public LabelConfig(String labelType, String property, Collection<Double> splitRates) {
            this.labelType = labelType;
            this.property = property;
            this.splitRates = splitRates;

            if (this.splitRates.size() != 3) {
                throw new IllegalArgumentException("splitRates must contain 3 values");
            }

            Optional<Double> sum = this.splitRates.stream().reduce(Double::sum);

            if (sum.orElse(0.0) != 1.0) {
                throw new IllegalArgumentException("splitRates values must add up to 1.0");
            }
        }

        public String property() {
            return property;
        }

        public Collection<Double> splitRates() {
            return splitRates;
        }

        public String labelType() {
            return labelType;
        }
    }

    public static class Range {
        private final Object low;
        private final Object high;

        public Range(Object low, Object high) {

            DataType lowDataType = DataType.dataTypeFor(low.getClass());
            DataType highDataType = DataType.dataTypeFor(high.getClass());

            if (!lowDataType.isNumeric() || !highDataType.isNumeric()) {
                throw new IllegalArgumentException("Low and high values must be numeric");
            }

            DataType dataType = DataType.getBroadestType(lowDataType, highDataType);

            Object highValue = dataType.convert(high);
            Object lowValue = dataType.convert(low);

            this.high = dataType.compare(highValue, lowValue) >= 0 ? highValue : lowValue;
            this.low = dataType.compare(highValue, lowValue) >= 0 ? lowValue : highValue;
        }

        public Object low() {
            return low;
        }

        public Object high() {
            return high;
        }

        @Override
        public String toString() {
            return "Range{" +
                    "low=" + low +
                    ", high=" + high +
                    '}';
        }
    }

    public static class NumericalBucketFeatureConfig {
        private final Label label;
        private final String property;
        private final Range range;
        private final int bucketCount;
        private final int slideWindowSize;

        public NumericalBucketFeatureConfig(Label label,
                                            String property,
                                            Range range,
                                            int bucketCount,
                                            int slideWindowSize) {

            this.label = label;
            this.property = property;
            this.range = range;
            this.bucketCount = bucketCount;
            this.slideWindowSize = slideWindowSize;
        }

        public Label label() {
            return label;
        }

        public String property() {
            return property;
        }

        public int bucketCount() {
            return bucketCount;
        }

        public int slideWindowSize() {
            return slideWindowSize;
        }

        public Range range() {
            return range;
        }

        @Override
        public String toString() {
            return "NumericalBucketFeatureConfig{" +
                    "label=" + label +
                    ", property='" + property + '\'' +
                    ", range=" + range +
                    ", bucketCount=" + bucketCount +
                    ", slideWindowSize=" + slideWindowSize +
                    '}';
        }
    }

    public static class FeatureOverrideConfig {
        private final Label label;
        private final String property;
        private final String featureType;
        private final Norm norm;
        private final String separator;

        public FeatureOverrideConfig(Label label, String property, String featureType, Norm norm, String separator) {
            this.label = label;
            this.property = property;
            this.featureType = featureType;
            this.norm = norm;
            this.separator = separator;
        }

        public Label label() {
            return label;
        }

        public String property() {
            return property;
        }

        public String featureType() {
            return featureType;
        }

        public Norm norm() {
            return norm;
        }

        public String separator() {
            return separator;
        }
    }

}
