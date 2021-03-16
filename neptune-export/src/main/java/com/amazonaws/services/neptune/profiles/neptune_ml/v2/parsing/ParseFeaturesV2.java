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

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Norm;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.*;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.*;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ParseFeaturesV2 {

    private final Collection<JsonNode> features;

    public ParseFeaturesV2(Collection<JsonNode> features) {
        this.features = features;
    }

    public void validate() {
        for (JsonNode feature : features) {
            if (!isTfIdfNodeFeature(feature) &&
                    !isDatetimeNodeFeature(feature) &&
                    !isAutoNodeFeature(feature) &&
                    !isWord2VecNodeFeature(feature) &&
                    !isNumericalBucketFeature(feature) &&
                    !isNodeFeatureOverride(feature) &&
                    !isEdgeFeatureOverride(feature)) {
                throw new IllegalArgumentException("Illegal feature element: expected auto, category, numerical, bucket_numerical, datetime, text_word2vec, or text_tfidf for nodes, auto or numerical for edges.");
            }
        }
    }

    public Collection<TfIdfConfigV2> parseTfIdfNodeFeatures() {
        Collection<TfIdfConfigV2> tfIdfFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (isTfIdfNodeFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.text_tfidf.name() + " feature");
                Label nodeType = new ParseNodeType(json, context).parseNodeType();
                String property = new ParseProperty(json, context.withLabel(nodeType)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(nodeType).withProperty(property);
                Range ngramRange = new ParseRange(json, "ngram_range", propertyContext).parseRange();
                int minDf = new ParseMinDfV2(json, propertyContext).parseMinDf();
                int maxFeatures = new ParseMaxFeaturesV2(json, propertyContext).parseMaxFeatures();
                TfIdfConfigV2 config = new TfIdfConfigV2(nodeType, property, ngramRange, minDf, maxFeatures);
                tfIdfFeatures.add(config);
            }
        }
        return tfIdfFeatures;
    }

    public Collection<DatetimeConfigV2> parseDatetimeFeatures() {
        Collection<DatetimeConfigV2> datetimeFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (isDatetimeNodeFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.datetime.name() + " feature");
                Label nodeType = new ParseNodeType(json, context).parseNodeType();
                String property = new ParseProperty(json, context.withLabel(nodeType)).parseSingleProperty();
                Collection<DatetimePartV2> datetimeParts = new ParseDatetimePartsV2(json, context.withLabel(nodeType).withProperty(property)).parseDatetimeParts();
                DatetimeConfigV2 config = new DatetimeConfigV2(nodeType, property, datetimeParts);
                datetimeFeatures.add(config);
            }
        }
        return datetimeFeatures;
    }

    public Collection<Word2VecConfig> parseWord2VecNodeFeatures() {
        Collection<Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (isWord2VecNodeFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.text_word2vec.name() + " feature");
                Label nodeType = new ParseNodeType(json, context).parseNodeType();
                String property = new ParseProperty(json, context.withLabel(nodeType)).parseSingleProperty();
                String language = new ParseLanguage(json).parseLanguage();
                Word2VecConfig config = new Word2VecConfig(nodeType, property, Collections.singletonList(language));
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<NumericalBucketFeatureConfigV2> parseNumericalBucketFeatures() {
        Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (isNumericalBucketFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.bucket_numerical.name() + " feature");

                Label nodeType = new ParseNodeType(json, context).parseNodeType();
                FeatureTypeV2.bucket_numerical.validateOverride(json, context);

                String property = new ParseProperty(json, context.withLabel(nodeType)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(nodeType).withProperty(property);
                Range range = new ParseRange(json, "range", propertyContext).parseRange();
                int bucketCount = new ParseBucketCountV2(json, propertyContext).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSize(json, propertyContext).parseSlideWindowSize();
                ImputerTypeV2 imputerType = new ParseImputerTypeV2(json, propertyContext).parseImputerType();

                NumericalBucketFeatureConfigV2 config = new NumericalBucketFeatureConfigV2(nodeType, property, range, bucketCount, slideWindowSize, imputerType);

                numericalBucketFeatures.add(config);
            }
        }
        return numericalBucketFeatures;
    }

    public Collection<FeatureOverrideConfigV2> parseNodeFeatureOverrides() {
        Collection<FeatureOverrideConfigV2> featureOverrides = new ArrayList<>();
        for (JsonNode json : features) {
            if (isNodeFeatureOverride(json)) {

                ParsingContext context = new ParsingContext("node feature");
                Label nodeType = new ParseNodeType(json, context).parseNodeType();
                Collection<String> properties = new ParseProperty(json, context.withLabel(nodeType)).parseMultipleProperties();
                ParsingContext propertiesContext = context.withLabel(nodeType).withProperties(properties);
                FeatureTypeV2 type = new ParseFeatureTypeV2(json, propertiesContext).parseFeatureType();

                type.validateOverride(json, context);

                Norm norm = new ParseNorm(json, propertiesContext).parseNorm();
                Separator separator = new ParseSeparator(json).parseSeparator();
                ImputerTypeV2 imputerType = new ParseImputerTypeV2(json, propertiesContext).parseImputerType();

                FeatureOverrideConfigV2 config = new FeatureOverrideConfigV2(nodeType, properties, type, norm, separator, imputerType);

                featureOverrides.add(config);
            }
        }
        return featureOverrides;
    }

    public Collection<FeatureOverrideConfigV2> parseEdgeFeatureOverrides() {
        Collection<FeatureOverrideConfigV2> featureOverrides = new ArrayList<>();
        for (JsonNode node : features) {
            if (isEdgeFeatureOverride(node)) {
                ParsingContext context = new ParsingContext("edge feature");
                Label edgeType = new ParseEdgeType(node, context).parseEdgeType();
                Collection<String> properties = new ParseProperty(node, context.withLabel(edgeType)).parseMultipleProperties();
                ParsingContext propertiesContext = context.withLabel(edgeType).withProperties(properties);
                FeatureTypeV2 type = new ParseFeatureTypeV2(node, propertiesContext).parseFeatureType();
                type.validateOverride(node, context);
                Norm norm = new ParseNorm(node, propertiesContext).parseNorm();
                Separator separator = new ParseSeparator(node).parseSeparator();
                ImputerTypeV2 imputerType = new ParseImputerTypeV2(node, context).parseImputerType();
                featureOverrides.add(new FeatureOverrideConfigV2(edgeType, properties, type, norm, separator, imputerType));
            }
        }
        return featureOverrides;
    }

    private boolean isTfIdfNodeFeature(JsonNode node) {
        return isNodeFeature(node) && isTfIdfType(node.get("type").textValue());
    }

    private boolean isDatetimeNodeFeature(JsonNode node) {
        return isNodeFeature(node) && isDatetimeType(node.get("type").textValue());
    }

    private boolean isAutoNodeFeature(JsonNode node) {
        return isNodeFeature(node) && isAutoType(node.get("type").textValue());
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
            return (isNumericalType(type) || isCategoricalType(type) || isAutoType(type));
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

    private boolean isTfIdfType(String type) {
        return FeatureTypeV2.text_tfidf.name().equals(type);
    }

    private boolean isDatetimeType(String type) {
        return FeatureTypeV2.datetime.name().equals(type);
    }

    private boolean isAutoType(String type) {
        return FeatureTypeV2.auto.name().equals(type);
    }

    private boolean isWord2VecType(String type) {
        return FeatureTypeV2.text_word2vec.name().equals(type);
    }

    private boolean isBucketNumericalType(String type) {
        return FeatureTypeV2.bucket_numerical.name().equals(type);
    }

    private boolean isCategoricalType(String type) {
        return FeatureTypeV2.category.name().equals(type);
    }

    private boolean isNumericalType(String type) {
        return FeatureTypeV2.numerical.name().equals(type);
    }
}
