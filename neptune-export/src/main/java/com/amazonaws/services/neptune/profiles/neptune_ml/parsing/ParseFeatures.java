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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ParseFeatures {

    private final Collection<JsonNode> nodes;

    public ParseFeatures(Collection<JsonNode> nodes) {
        this.nodes = nodes;
    }

    public void validate() {
        for (JsonNode node : nodes) {
            if (!isWord2VecNodeFeature(node) &&
                    !isNumericalBucketFeature(node) &&
                    !isNodeFeatureOverride(node) &&
                    !isEdgeFeatureOverride(node)) {
                throw new IllegalArgumentException("Illegal feature element: expected category or numerical feature definitions for nodes and edges, or word2vec or bucket_numerical feature definitions for nodes");
            }
        }
    }

    public Collection<TrainingJobWriterConfig.Word2VecConfig> parseWord2VecNodeFeatures() {
        Collection<TrainingJobWriterConfig.Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isWord2VecNodeFeature(node)) {
                String description = "word2vec feature";
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String property = new ParseProperty(node, description).parseSingleProperty();
                String language = new ParseLanguage(node).parseLanguage();
                TrainingJobWriterConfig.Word2VecConfig config = new TrainingJobWriterConfig.Word2VecConfig(nodeType, property, Collections.singletonList(language));
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> parseNumericalBucketFeatures() {
        Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNumericalBucketFeature(node)) {
                String description = "bucket_numerical feature";
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                FeatureType.bucket_numerical.validateHint(node, description, nodeType);
                String property = new ParseProperty(node, description).parseSingleProperty();
                TrainingJobWriterConfig.Range range = new ParseRange(node, description).parseRange();
                int bucketCount = new ParseBucketCount(node, description).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSize(node, description).parseSlideWindowSize();
                TrainingJobWriterConfig.NumericalBucketFeatureConfig config = new TrainingJobWriterConfig.NumericalBucketFeatureConfig(nodeType, property, range, bucketCount, slideWindowSize);
                numericalBucketFeatures.add(config);
            }
        }
        return numericalBucketFeatures;
    }

    public Collection<TrainingJobWriterConfig.FeatureOverrideConfig> parseNodeFeatureOverrides() {
        Collection<TrainingJobWriterConfig.FeatureOverrideConfig> featureOverrides = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNodeFeatureOverride(node)) {
                String description = "node feature";
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                Collection<String> properties = new ParseProperty(node, description).parseMultipleProperties();
                FeatureType type = new ParseFeatureType(node, description).parseFeatureType();
                type.validateHint(node, description, nodeType);
                Norm norm = new ParseNorm(node).parseNorm();
                String separator = new ParseSeparator(node).parseSeparator();
                TrainingJobWriterConfig.FeatureOverrideConfig config = new TrainingJobWriterConfig.FeatureOverrideConfig(nodeType, properties, type, norm, separator);
                featureOverrides.add(config);
            }
        }
        return featureOverrides;
    }

    public Collection<TrainingJobWriterConfig.FeatureOverrideConfig> parseEdgeFeatureOverrides() {
        Collection<TrainingJobWriterConfig.FeatureOverrideConfig> featureOverrides = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isEdgeFeatureOverride(node)) {
                String description = "edge feature";
                Label edgeType = new ParseEdgeType(node, description).parseEdgeType();
                Collection<String> properties = new ParseProperty(node, description).parseMultipleProperties();
                FeatureType type = new ParseFeatureType(node, description).parseFeatureType();
                type.validateHint(node, description, edgeType);
                Norm norm = new ParseNorm(node).parseNorm();
                String separator = new ParseSeparator(node).parseSeparator();
                featureOverrides.add(new TrainingJobWriterConfig.FeatureOverrideConfig(edgeType, properties, type, norm, separator));
            }
        }
        return featureOverrides;
    }

    private boolean isWord2VecNodeFeature(JsonNode node) {
        return isNodeFeature(node) && isWord2VecType(node.get("type").textValue());
    }

    private boolean isNumericalBucketFeature(JsonNode node) {
        return isNodeFeature(node) && isBucketNumericalType(node.get("type").textValue());
    }

    private boolean isNodeFeatureOverride(JsonNode node) {
        if (isNodeFeature(node)) {
            String type = node.get("type").textValue();
            return (isNumericalType(type) || isCategoricalType(type));
        }
        return false;
    }

    private boolean isEdgeFeatureOverride(JsonNode node) {
        if (isEdgeFeature(node)) {
            String type = node.get("type").textValue();
            return (isNumericalType(type) || isCategoricalType(type));
        }
        return false;
    }

    private boolean isNodeFeature(JsonNode node) {
        return node.has("node") && node.has("type");
    }

    private boolean isEdgeFeature(JsonNode node) {
        return node.has("edge") && node.has("type");
    }

    private boolean isWord2VecType(String type){
        return FeatureType.word2vec.name().equals(type);
    }

    private boolean isBucketNumericalType(String type){
        return FeatureType.bucket_numerical.name().equals(type);
    }

    private boolean isCategoricalType(String type) {
        return FeatureType.category.name().equals(type);
    }

    private boolean isNumericalType(String type) {
        return FeatureType.numerical.name().equals(type);
    }


}
