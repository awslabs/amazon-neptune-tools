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

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Norm;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.*;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.FeatureOverrideConfigV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.FeatureTypeV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.NumericalBucketFeatureConfigV1;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

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

    public Collection<Word2VecConfig> parseWord2VecNodeFeatures() {
        Collection<Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isWord2VecNodeFeature(node)) {
                String description = "word2vec feature";
                ParsingContext context = new ParsingContext(FeatureTypeV1.word2vec.name() + " feature");
                Label nodeType = new ParseNodeType(node, context).parseNodeType();
                String property = new ParseProperty(node, context.withLabel(nodeType)).parseSingleProperty();
                Collection<String> language = new ParseWord2VecLanguage(node).parseLanguage();
                Word2VecConfig config = new Word2VecConfig(nodeType, property, language);
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<NumericalBucketFeatureConfigV1> parseNumericalBucketFeatures() {
        Collection<NumericalBucketFeatureConfigV1> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNumericalBucketFeature(node)) {
                String description = FeatureTypeV1.bucket_numerical.name();
                ParsingContext context = new ParsingContext(FeatureTypeV1.bucket_numerical.name() + " feature");
                Label nodeType = new ParseNodeType(node, context).parseNodeType();
                FeatureTypeV1.bucket_numerical.validateOverride(node, description, nodeType);
                String property = new ParseProperty(node, context.withLabel(nodeType)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(nodeType).withProperty(property);
                Range range = new ParseRange(node, "range", propertyContext).parseRange();
                int bucketCount = new ParseBucketCountV1(node, propertyContext).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSize(node, propertyContext).parseSlideWindowSize();
                NumericalBucketFeatureConfigV1 config = new NumericalBucketFeatureConfigV1(nodeType, property, range, bucketCount, slideWindowSize);
                numericalBucketFeatures.add(config);
            }
        }
        return numericalBucketFeatures;
    }

    public Collection<FeatureOverrideConfigV1> parseNodeFeatureOverrides() {
        Collection<FeatureOverrideConfigV1> featureOverrides = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNodeFeatureOverride(node)) {
                String description = "node feature";
                ParsingContext context = new ParsingContext("node feature");
                Label nodeType = new ParseNodeType(node, context).parseNodeType();
                Collection<String> properties = new ParseProperty(node, context.withLabel(nodeType)).parseMultipleProperties();
                ParsingContext propertiesContext = context.withLabel(nodeType).withProperties(properties);
                FeatureTypeV1 type = new ParseFeatureTypeV1(node, propertiesContext).parseFeatureType();
                type.validateOverride(node, description, nodeType);
                Norm norm = new ParseNorm(node, propertiesContext).parseNorm();
                Separator separator = new ParseSeparator(node).parseSeparator();
                FeatureOverrideConfigV1 config = new FeatureOverrideConfigV1(nodeType, properties, type, norm, separator);
                featureOverrides.add(config);
            }
        }
        return featureOverrides;
    }

    public Collection<FeatureOverrideConfigV1> parseEdgeFeatureOverrides() {
        Collection<FeatureOverrideConfigV1> featureOverrides = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isEdgeFeatureOverride(node)) {
                ParsingContext context = new ParsingContext("edge feature");
                String description = "edge feature";
                Label edgeType = new ParseEdgeType(node, context).parseEdgeType();
                Collection<String> properties = new ParseProperty(node, context.withLabel(edgeType)).parseMultipleProperties();
                ParsingContext propertiesContext = context.withLabel(edgeType).withProperties(properties);
                FeatureTypeV1 type = new ParseFeatureTypeV1(node, propertiesContext).parseFeatureType();
                type.validateOverride(node, description, edgeType);
                Norm norm = new ParseNorm(node, propertiesContext).parseNorm();
                Separator separator = new ParseSeparator(node).parseSeparator();
                featureOverrides.add(new FeatureOverrideConfigV1(edgeType, properties, type, norm, separator));
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

    private boolean isWord2VecType(String type) {
        return FeatureTypeV1.word2vec.name().equals(type);
    }

    private boolean isBucketNumericalType(String type) {
        return FeatureTypeV1.bucket_numerical.name().equals(type);
    }

    private boolean isCategoricalType(String type) {
        return FeatureTypeV1.category.name().equals(type);
    }

    private boolean isNumericalType(String type) {
        return FeatureTypeV1.numerical.name().equals(type);
    }


}
