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

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.*;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.*;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.*;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class ParseFeaturesV2 {

    private final Collection<JsonNode> features;

    public ParseFeaturesV2(Collection<JsonNode> features) {
        this.features = features;
    }

    public interface ElementFeatureFilter {
        boolean isCorrectType(JsonNode json);
    }

    public interface LabelSupplier {
        Label getLabel(JsonNode json, ParsingContext context);
    }

    public static ElementFeatureFilter NodeFeatureFilter = json -> json.has("node") && json.has("type");

    public static ElementFeatureFilter EdgeFeatureFilter = json -> json.has("edge") && json.has("type");

    public static LabelSupplier NodeLabelSupplier = (json, context) -> new ParseNodeType(json, context).parseNodeType();

    public static LabelSupplier EdgeLabelSupplier = (json, context) -> new ParseEdgeType(json, context).parseEdgeType();

    public void validate() {
        for (JsonNode feature : features) {
            if (!isNoneFeature(feature) &&
                    !isTfIdfFeature(feature) &&
                    !isDatetimeFeature(feature) &&
                    !isAutoFeature(feature) &&
                    !isWord2VecFeature(feature) &&
                    !isFastTextFeature(feature) &&
                    !isSbertTextFeature(feature) &&
                    !isNumericalBucketFeature(feature) &&
                    !isNodeFeatureOverride(feature) &&
                    !isEdgeFeatureOverride(feature)) {
                if (feature.has("type")) {
                    String featureType = feature.get("type").textValue();
                    throw new IllegalArgumentException(
                            String.format("Illegal feature type: '%s'. Supported values are: %s.",
                                    featureType,
                                    ErrorMessageHelper.quoteList(FeatureTypeV2.publicFormattedNames())));
                }
            }
        }
    }

    public Collection<NoneFeatureConfig> parseNoneFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<NoneFeatureConfig> noneFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isNoneFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.none.name() + " feature");
                Label label = supplier.getLabel(json, context);
                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                NoneFeatureConfig config = new NoneFeatureConfig(label, property);
                noneFeatures.add(config);
            }
        }
        return noneFeatures;
    }

    public Collection<TfIdfConfigV2> parseTfIdfFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<TfIdfConfigV2> tfIdfFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isTfIdfFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.text_tfidf.name() + " feature");
                Label label = supplier.getLabel(json, context);
                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(label).withProperty(property);
                Range ngramRange = new ParseRange(json, "ngram_range", propertyContext).parseRange();
                int minDf = new ParseMinDfV2(json, propertyContext).parseMinDf();
                int maxFeatures = new ParseMaxFeaturesV2(json, propertyContext).parseMaxFeatures();
                TfIdfConfigV2 config = new TfIdfConfigV2(label, property, ngramRange, minDf, maxFeatures);
                tfIdfFeatures.add(config);
            }
        }
        return tfIdfFeatures;
    }

    public Collection<DatetimeConfigV2> parseDatetimeFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<DatetimeConfigV2> datetimeFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isDatetimeFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.datetime.name() + " feature");
                Label label = supplier.getLabel(json, context);
                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                Collection<DatetimePartV2> datetimeParts = new ParseDatetimePartsV2(json, context.withLabel(label).withProperty(property)).parseDatetimeParts();
                DatetimeConfigV2 config = new DatetimeConfigV2(label, property, datetimeParts);
                datetimeFeatures.add(config);
            }
        }
        return datetimeFeatures;
    }

    public Collection<Word2VecConfig> parseWord2VecFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isWord2VecFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.text_word2vec.name() + " feature");
                Label label = supplier.getLabel(json, context);
                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                Collection<String> language = new ParseWord2VecLanguage(json).parseLanguage();
                Word2VecConfig config = new Word2VecConfig(label, property, language);
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<SbertConfig> parseSbertFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<SbertConfig> sbertConfigs = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isSbertTextFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.text_sbert.name() + " feature");
                Label label = supplier.getLabel(json, context);
                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                String name = new ParseSbertTypeName(json, context).parseTypeName();
                SbertConfig config = new SbertConfig(label, property, name);
                sbertConfigs.add(config);
            }
        }
        return sbertConfigs;
    }

    public Collection<FastTextConfig> parseFastTextFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<FastTextConfig> fastTextFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isFastTextFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.text_fasttext.name() + " feature");
                Label label = supplier.getLabel(json, context);
                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                String language = new ParseFastTextLanguage(json, context).parseLanguage();
                Integer maxLength = new ParseMaxLength(json, context).parseMaxLength();
                FastTextConfig config = new FastTextConfig(label, property, language, maxLength);
                fastTextFeatures.add(config);
            }
        }
        return fastTextFeatures;
    }

    public Collection<NumericalBucketFeatureConfigV2> parseNumericalBucketFeatures(ElementFeatureFilter filter, LabelSupplier supplier) {
        Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode json : features) {
            if (filter.isCorrectType(json) && isNumericalBucketFeature(json)) {
                ParsingContext context = new ParsingContext(FeatureTypeV2.bucket_numerical.name() + " feature");

                Label label = supplier.getLabel(json, context);
                FeatureTypeV2.bucket_numerical.validateOverride(json, context);

                String property = new ParseProperty(json, context.withLabel(label)).parseSingleProperty();
                ParsingContext propertyContext = context.withLabel(label).withProperty(property);
                Range range = new ParseRange(json, "range", propertyContext).parseRange();
                int bucketCount = new ParseBucketCountV2(json, propertyContext).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSize(json, propertyContext).parseSlideWindowSize();
                ImputerTypeV2 imputerType = new ParseImputerTypeV2(json, propertyContext).parseImputerType();

                NumericalBucketFeatureConfigV2 config = new NumericalBucketFeatureConfigV2(label, property, range, bucketCount, slideWindowSize, imputerType);

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

                FeatureOverrideConfigV2 config = new FeatureOverrideConfigV2(edgeType, properties, type, norm, separator, imputerType);

                featureOverrides.add(config);
            }
        }
        return featureOverrides;
    }

    private boolean isNoneFeature(JsonNode node) {
        return isNoneFeatureType(node.get("type").textValue());
    }

    private boolean isTfIdfFeature(JsonNode node) {
        return isTfIdfType(node.get("type").textValue());
    }

    private boolean isDatetimeFeature(JsonNode node) {
        return isDatetimeType(node.get("type").textValue());
    }

    private boolean isAutoFeature(JsonNode node) {
        return isAutoType(node.get("type").textValue());
    }

    private boolean isWord2VecFeature(JsonNode node) {
        return isWord2VecType(node.get("type").textValue());
    }

    private boolean isFastTextFeature(JsonNode node) {
        return isFastTextType(node.get("type").textValue());
    }

    private boolean isSbertTextFeature(JsonNode node) {
        return isSbertTextType(node.get("type").textValue());
    }

    private boolean isNumericalBucketFeature(JsonNode node) {
        return isBucketNumericalType(node.get("type").textValue());
    }

    private boolean isNodeFeatureOverride(JsonNode node) {
        if (isNodeFeature(node)) {
            String type = node.get("type").textValue();
            return (isNumericalType(type) || isCategoricalType(type) || isAutoType(type) || isNoneType(type));
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

    private boolean isNoneFeatureType(String type) {
        return isOfType(FeatureTypeV2.none, type);
    }

    private boolean isTfIdfType(String type) {
        return isOfType(FeatureTypeV2.text_tfidf, type);
    }

    private boolean isDatetimeType(String type) {
        return isOfType(FeatureTypeV2.datetime, type);
    }

    private boolean isAutoType(String type) {
        return isOfType(FeatureTypeV2.auto, type);
    }

    private boolean isWord2VecType(String type) {
        return isOfType(FeatureTypeV2.text_word2vec, type);
    }

    private boolean isFastTextType(String type) {
        return isOfType(FeatureTypeV2.text_fasttext, type);
    }

    private boolean isSbertTextType(String type) {
        return isOfType(FeatureTypeV2.text_sbert, type) ||
                isOfType(FeatureTypeV2.text_sbert128, type) ||
                isOfType(FeatureTypeV2.text_sbert512, type);
    }

    private boolean isBucketNumericalType(String type) {
        return isOfType(FeatureTypeV2.bucket_numerical, type);
    }

    private boolean isCategoricalType(String type) {
        return isOfType(FeatureTypeV2.category, type);
    }

    private boolean isNumericalType(String type) {
        return isOfType(FeatureTypeV2.numerical, type);
    }

    private boolean isNoneType(String type) {
        return isOfType(FeatureTypeV2.none, type);
    }

    private boolean isOfType(FeatureTypeV2 featureTypeV2, String s) {
        for (String validName : featureTypeV2.validNames()) {
            if (validName.equals(s)) {
                return true;
            }
        }
        return false;
    }
}
