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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ParseFeaturesV1 {

    private final Collection<JsonNode> nodes;

    public ParseFeaturesV1(Collection<JsonNode> nodes) {
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

    public Collection<TrainingJobWriterConfigV1.Word2VecConfig> parseWord2VecNodeFeatures() {
        Collection<TrainingJobWriterConfigV1.Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isWord2VecNodeFeature(node)) {
                String description = "word2vec feature";
                Label nodeType = new ParseNodeTypeV1(node, description).parseNodeType();
                String property = new ParsePropertyV1(node, description).parseSingleProperty();
                String language = new ParseLanguageV1(node).parseLanguage();
                TrainingJobWriterConfigV1.Word2VecConfig config = new TrainingJobWriterConfigV1.Word2VecConfig(nodeType, property, Collections.singletonList(language));
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<TrainingJobWriterConfigV1.NumericalBucketFeatureConfig> parseNumericalBucketFeatures() {
        Collection<TrainingJobWriterConfigV1.NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNumericalBucketFeature(node)) {
                String description = "bucket_numerical feature";
                Label nodeType = new ParseNodeTypeV1(node, description).parseNodeType();
                FeatureTypeV1.bucket_numerical.validateHint(node, description, nodeType);
                String property = new ParsePropertyV1(node, description).parseSingleProperty();
                TrainingJobWriterConfigV1.Range range = new ParseRangeV1(node, description).parseRange();
                int bucketCount = new ParseBucketCountV1(node, description).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSizeV1(node).parseSlideWindowSize();
                TrainingJobWriterConfigV1.NumericalBucketFeatureConfig config = new TrainingJobWriterConfigV1.NumericalBucketFeatureConfig(nodeType, property, range, bucketCount, slideWindowSize);
                numericalBucketFeatures.add(config);
            }
        }
        return numericalBucketFeatures;
    }

    public Collection<TrainingJobWriterConfigV1.FeatureOverrideConfig> parseNodeFeatureOverrides() {
        Collection<TrainingJobWriterConfigV1.FeatureOverrideConfig> featureOverrides = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNodeFeatureOverride(node)) {
                String description = "node feature";
                Label nodeType = new ParseNodeTypeV1(node, description).parseNodeType();
                Collection<String> properties = new ParsePropertyV1(node, description).parseMultipleProperties();
                FeatureTypeV1 type = new ParseFeatureTypeV1(node, description).parseFeatureType();
                type.validateHint(node, description, nodeType);
                NormV1 norm = new ParseNormV1(node).parseNorm();
                String separator = new ParseSeparatorV1(node).parseSeparator();
                TrainingJobWriterConfigV1.FeatureOverrideConfig config = new TrainingJobWriterConfigV1.FeatureOverrideConfig(nodeType, properties, type, norm, separator);
                featureOverrides.add(config);
            }
        }
        return featureOverrides;
    }

    public Collection<TrainingJobWriterConfigV1.FeatureOverrideConfig> parseEdgeFeatureOverrides() {
        Collection<TrainingJobWriterConfigV1.FeatureOverrideConfig> featureOverrides = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isEdgeFeatureOverride(node)) {
                String description = "edge feature";
                Label edgeType = new ParseEdgeTypeV1(node, description).parseEdgeType();
                Collection<String> properties = new ParsePropertyV1(node, description).parseMultipleProperties();
                FeatureTypeV1 type = new ParseFeatureTypeV1(node, description).parseFeatureType();
                type.validateHint(node, description, edgeType);
                NormV1 norm = new ParseNormV1(node).parseNorm();
                String separator = new ParseSeparatorV1(node).parseSeparator();
                featureOverrides.add(new TrainingJobWriterConfigV1.FeatureOverrideConfig(edgeType, properties, type, norm, separator));
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
        return FeatureTypeV1.word2vec.name().equals(type);
    }

    private boolean isBucketNumericalType(String type){
        return FeatureTypeV1.bucket_numerical.name().equals(type);
    }

    private boolean isCategoricalType(String type) {
        return FeatureTypeV1.category.name().equals(type);
    }

    private boolean isNumericalType(String type) {
        return FeatureTypeV1.numerical.name().equals(type);
    }


}
