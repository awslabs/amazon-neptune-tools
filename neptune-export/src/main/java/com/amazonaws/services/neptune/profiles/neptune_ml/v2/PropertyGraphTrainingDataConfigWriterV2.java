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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2;

import com.amazonaws.services.neptune.profiles.neptune_ml.PropertyName;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.*;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ErrorMessageHelper;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.*;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class PropertyGraphTrainingDataConfigWriterV2 {

    public static final PropertyName COLUMN_NAME_WITH_DATATYPE = new PropertyName() {
        @Override
        public String escaped(PropertySchema propertySchema, PrinterOptions printerOptions) {
            return propertySchema.nameWithDataType(printerOptions.csv().escapeCsvHeaders());
        }

        @Override
        public String unescaped(PropertySchema propertySchema) {
            return propertySchema.nameWithDataType();
        }
    };

    public static final PropertyName COLUMN_NAME_WITHOUT_DATATYPE = new PropertyName() {
        @Override
        public String escaped(PropertySchema propertySchema, PrinterOptions printerOptions) {
            return propertySchema.nameWithoutDataType(printerOptions.csv().escapeCsvHeaders());
        }

        @Override
        public String unescaped(PropertySchema propertySchema) {
            return propertySchema.nameWithoutDataType();
        }
    };

    private final GraphSchema graphSchema;
    private final JsonGenerator generator;
    private final PropertyName propertyName;
    private final PrinterOptions printerOptions;
    private final TrainingDataWriterConfigV2 config;
    private final Collection<String> warnings = new ArrayList<>();

    public PropertyGraphTrainingDataConfigWriterV2(GraphSchema graphSchema,
                                                   JsonGenerator generator,
                                                   PropertyName propertyName,
                                                   PrinterOptions printerOptions) {
        this(graphSchema, generator, propertyName, printerOptions, new TrainingDataWriterConfigV2());
    }

    public PropertyGraphTrainingDataConfigWriterV2(GraphSchema graphSchema,
                                                   JsonGenerator generator,
                                                   PropertyName propertyName,
                                                   PrinterOptions printerOptions,
                                                   TrainingDataWriterConfigV2 config) {
        this.graphSchema = graphSchema;
        this.generator = generator;
        this.propertyName = propertyName;
        this.printerOptions = printerOptions;
        this.config = config;
    }

    public void write() throws IOException {

        generator.writeStartObject();

        generator.writeStringField("version", "v2.0");
        generator.writeStringField("query_engine", "gremlin");

        generator.writeObjectFieldStart("graph");
        writeNodes();
        writeEdges();
        generator.writeEndObject();

        generator.writeArrayFieldStart("warnings");
        writeWarnings();
        generator.writeEndArray();

        generator.writeEndObject();

        generator.flush();
    }

    private void writeWarnings() throws IOException {
        for (String warning : warnings) {
            generator.writeString(warning);
        }
    }

    private void writeNodes() throws IOException {

        GraphElementType graphElementType = GraphElementType.nodes;
        GraphElementSchemas graphElementSchemas = graphSchema.graphElementSchemasFor(graphElementType);

        generator.writeArrayFieldStart("nodes");

        for (Label nodeLabel : graphElementSchemas.labels()) {

            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(nodeLabel);
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(nodeLabel);

            for (String outputId : outputIds) {
                generator.writeStartObject();

                writeFileName(graphElementType, outputId);
                writeCommaSeparator();
                writeNodeType(labelSchema);
                writeNodeFeatures(labelSchema);
                writeNodeLabels(labelSchema);

                generator.writeEndObject();
            }
        }

        generator.writeEndArray();
    }


    private void writeEdges() throws IOException {

        GraphElementType graphElementType = GraphElementType.edges;
        GraphElementSchemas graphElementSchemas = graphSchema.graphElementSchemasFor(graphElementType);

        generator.writeArrayFieldStart("edges");

        for (Label edgeLabel : graphElementSchemas.labels()) {

            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(edgeLabel);
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(edgeLabel);

            for (String outputId : outputIds) {
                generator.writeStartObject();

                writeFileName(graphElementType, outputId);
                writeCommaSeparator();
                writeEdgeType(labelSchema);
                writeEdgeLabels(labelSchema);

                generator.writeEndObject();
            }
        }

        generator.writeEndArray();
    }

    private void writeNodeType(LabelSchema labelSchema) throws IOException {
        generator.writeArrayFieldStart("node");

        generator.writeString("~id");
        generator.writeString(labelSchema.label().labelsAsString());

        generator.writeEndArray();
    }

    private void writeEdgeType(LabelSchema labelSchema) throws IOException {
        generator.writeArrayFieldStart("source");
        generator.writeString("~from");
        generator.writeString(labelSchema.label().fromLabelsAsString());
        generator.writeEndArray();

        generator.writeArrayFieldStart("relation");
        generator.writeString("");
        generator.writeString(labelSchema.label().labelsAsString());
        generator.writeEndArray();

        generator.writeArrayFieldStart("dest");
        generator.writeString("~to");
        generator.writeString(labelSchema.label().toLabelsAsString());
        generator.writeEndArray();
    }

    private void writeNodeFeatures(LabelSchema labelSchema) throws IOException {

        Label label = labelSchema.label();
        Collection<PropertySchema> propertySchemas = labelSchema.propertySchemas();

        generator.writeArrayFieldStart("features");

        for (PropertySchema propertySchema : propertySchemas) {
            String column = propertySchema.nameWithoutDataType();
            if (config.hasNodeClassificationSpecificationForNodeProperty(label, column)) {
                continue;
            }
            if (config.allowAutoInferNodeFeature(label, column)) {
                writeAutoInferredNodeFeature(propertySchema);
            }
            if (config.hasTfIdfSpecification(label, column)) {
                writeTfIdfNodeFeature(propertySchema, config.getTfIdfSpecification(label, column));
            }
            if (config.hasDatetimeSpecification(label, column)) {
                writeDatetimeNodeFeature(propertySchema, config.getDatetimeSpecification(label, column));
            }
            if (config.hasWord2VecSpecification(label, column)) {
                writeWord2VecNodeFeature(propertySchema, config.getWord2VecSpecification(label, column));
            }
            if (config.hasNumericalBucketSpecification(label, column)) {
                writeNumericalBucketNodeFeature(propertySchema, config.getNumericalBucketSpecification(label, column));
            }
        }

        for (FeatureOverrideConfigV2 featureOverride : config.getNodeFeatureOverrides(label)) {
            writeNodeFeatureOverride(labelSchema, featureOverride);
        }

        generator.writeEndArray();
    }

    private void writeNodeLabels(LabelSchema labelSchema) throws IOException {
        Label label = labelSchema.label();

        if (config.hasNodeClassificationSpecificationsForNode(label)) {
            generator.writeArrayFieldStart("labels");
            for (LabelConfigV2 labelConfig : config.getNodeClassificationSpecificationsForNode(label)) {
                if (labelSchema.containsProperty(labelConfig.property())) {
                    PropertySchema propertySchema = labelSchema.getPropertySchema(labelConfig.property());
                    writeLabel(propertySchema, labelConfig);
                } else {
                    ParsingContext context = new ParsingContext("node classification property").withLabel(label).withProperty(labelConfig.property());
                    warnings.add(String.format("Unrecognized %s.", context));
                }
            }

            generator.writeEndArray();
        }
    }

    private void writeLabel(PropertySchema propertySchema, LabelConfigV2 labelConfig) throws IOException {
        generator.writeStartObject();
        generator.writeArrayFieldStart("label");
        generator.writeString(labelConfig.property());
        generator.writeString(labelConfig.type());
        generator.writeEndArray();
        writeSplitRates(labelConfig);
        labelConfig.separator().writeTo(generator, propertySchema.isMultiValue());
        generator.writeEndObject();
    }

    private void writeEdgeLabels(LabelSchema labelSchema) throws IOException {
        Label label = labelSchema.label();

        if (config.hasEdgeClassificationSpecificationsForEdge(label)) {
            generator.writeArrayFieldStart("labels");
            for (LabelConfigV2 labelConfig : config.getEdgeClassificationSpecificationsForEdge(label)) {
                if (StringUtils.isEmpty(labelConfig.property())) {
                    writeLabel(new PropertySchema(""), labelConfig);
                } else if (labelSchema.containsProperty(labelConfig.property())) {
                    PropertySchema propertySchema = labelSchema.getPropertySchema(labelConfig.property());
                    writeLabel(propertySchema, labelConfig);
                } else {
                    ParsingContext context = new ParsingContext("edge classification property").withLabel(label).withProperty(labelConfig.property());
                    warnings.add(String.format("Unrecognized %s.", context));
                }
            }

            generator.writeEndArray();
        }
    }

    private void writeSplitRates(LabelConfigV2 labelConfig) throws IOException {
        generator.writeArrayFieldStart("split_rate");
        for (Double rate : labelConfig.splitRates()) {
            generator.writeNumber(rate);
        }
        generator.writeEndArray();
    }

    private void writeNodeFeatureOverride(LabelSchema labelSchema, FeatureOverrideConfigV2 featureOverride) throws IOException {

        FeatureTypeV2 featureType = featureOverride.featureType();

        Label label = labelSchema.label();

        Collection<PropertySchema> propertySchemas = labelSchema.propertySchemas().stream()
                .filter(p -> featureOverride.properties().contains(p.nameWithoutDataType()) &&
                        !config.hasNodeClassificationSpecificationForNodeProperty(label, p.nameWithoutDataType()))
                .collect(Collectors.toList());

        Collection<String> propertyNames = propertySchemas.stream()
                .map(PropertySchema::nameWithoutDataType)
                .collect(Collectors.toList());

        Collection<String> missingProperties = featureOverride.properties().stream()
                .filter(p -> !propertyNames.contains(p))
                .collect(Collectors.toList());

        for (String missingProperty : missingProperties) {
            ParsingContext context = new ParsingContext(featureType.name() + " feature override").withLabel(label).withProperty(missingProperty);
            warnings.add(String.format("Unable to add %s. Property is missing, or is being used to label the node.", context));
        }

        if (FeatureTypeV2.category == featureType) {
            writeCategoricalNodeFeature(propertySchemas, featureOverride);
        } else if (FeatureTypeV2.numerical == featureType) {
            writeNumericalNodeFeature(propertySchemas, featureOverride);
        } else if (FeatureTypeV2.auto == featureType) {
            writeAutoFeature(propertySchemas, featureOverride);
        } else if (FeatureTypeV2.none == featureType) {
            // Do nothing
        } else {
            warnings.add(String.format("Unsupported feature type override for node: %s.", featureType.name()));
        }


    }

    private void writeAutoInferredNodeFeature(Collection<PropertySchema> propertySchemas) throws IOException {
        for (PropertySchema propertySchema : propertySchemas) {
            writeAutoInferredNodeFeature(propertySchema);
        }
    }

    private void writeAutoInferredNodeFeature(PropertySchema propertySchema) throws IOException {

        if (propertySchema.dataType() == DataType.String ||
                propertySchema.dataType() == DataType.Boolean) {
            writeAutoFeature(Collections.singletonList(propertySchema), ImputerTypeV2.none);
        }

        if (propertySchema.dataType() == DataType.Byte ||
                propertySchema.dataType() == DataType.Short ||
                propertySchema.dataType() == DataType.Integer ||
                propertySchema.dataType() == DataType.Long ||
                propertySchema.dataType() == DataType.Float ||
                propertySchema.dataType() == DataType.Double) {
            if (propertySchema.isMultiValue()) {
                writeAutoFeature(Collections.singletonList(propertySchema), ImputerTypeV2.median);
            } else {
                writeNumericalNodeFeature(
                        Collections.singletonList(propertySchema),
                        Norm.min_max,
                        ImputerTypeV2.median);
            }
        }

        if (propertySchema.dataType() == DataType.Date) {
            writeDatetimeNodeFeature(
                    Collections.singletonList(propertySchema),
                    Arrays.asList(
                            DatetimePartV2.year,
                            DatetimePartV2.month,
                            DatetimePartV2.weekday,
                            DatetimePartV2.hour));
        }
    }

    private void writeFeature(PropertySchema propertySchema, FeatureTypeV2 featureType) throws IOException {
        generator.writeArrayFieldStart("feature");
        generator.writeString(propertyName.escaped(propertySchema, printerOptions)); // column
        generator.writeString(propertyName.escaped(propertySchema, printerOptions)); // feature name
        generator.writeString(featureType.name());
        generator.writeEndArray();
    }

    private void writeTfIdfNodeFeature(PropertySchema propertySchema, TfIdfConfigV2 tfIdfSpecification) throws IOException {

        if (propertySchema.isMultiValue()) {
            warnings.add(String.format("%s feature does not support multi-value properties. Auto-inferring a feature for '%s'.", FeatureTypeV2.text_tfidf, propertySchema.nameWithoutDataType()));
            writeAutoInferredNodeFeature(propertySchema);
            return;
        }

        generator.writeStartObject();

        writeFeature(propertySchema, FeatureTypeV2.text_tfidf);

        Range range = tfIdfSpecification.ngramRange();

        if (range != null) {
            generator.writeArrayFieldStart("ngram_range");
            generator.writeObject(range.low());
            generator.writeObject(range.high());
            generator.writeEndArray();
        }

        Integer minDf = tfIdfSpecification.minDf();

        if (minDf != null) {
            generator.writeNumberField("min_df", minDf);
        }

        Integer maxFeatures = tfIdfSpecification.maxFeatures();

        if (maxFeatures != null) {
            generator.writeNumberField("max_features", maxFeatures);
        }

        generator.writeEndObject();
    }

    private void writeNumericalBucketNodeFeature(PropertySchema propertySchema, NumericalBucketFeatureConfigV2 numericalBucketSpecification) throws IOException {

        if (propertySchema.isMultiValue()) {
            warnings.add(String.format("%s feature does not support multi-value properties. Auto-inferring a feature for '%s'.", FeatureTypeV2.bucket_numerical, propertySchema.nameWithoutDataType()));
            writeAutoInferredNodeFeature(propertySchema);
            return;
        }

        generator.writeStartObject();

        writeFeature(propertySchema, FeatureTypeV2.bucket_numerical);

        Range range = numericalBucketSpecification.range();

        if (range != null) {
            generator.writeArrayFieldStart("range");
            generator.writeObject(range.low());
            generator.writeObject(range.high());
            generator.writeEndArray();
        }

        Integer bucketCount = numericalBucketSpecification.bucketCount();

        if (bucketCount != null) {
            generator.writeNumberField("bucket_cnt", bucketCount);
        }

        Integer slideWindowSize = numericalBucketSpecification.slideWindowSize();

        if (slideWindowSize != null) {
            generator.writeNumberField("slide_window_size", slideWindowSize);
        }

        ImputerTypeV2 imputer = numericalBucketSpecification.imputerType();

        if (imputer != null && imputer != ImputerTypeV2.none) {
            generator.writeStringField("imputer", imputer.formattedName());
        } else {
            warnings.add(String.format("'imputer' value missing for %s feature for '%s'. Preprocessing will exit when it encounters an missing value.", FeatureTypeV2.bucket_numerical, propertySchema.nameWithoutDataType()));
        }

        generator.writeEndObject();
    }

    private void writeWord2VecNodeFeature(PropertySchema propertySchema, Word2VecConfig word2VecSpecification) throws IOException {

        if (propertySchema.isMultiValue()) {
            warnings.add(String.format("%s feature does not support multi-value properties. Auto-inferring a feature for '%s'.", FeatureTypeV2.text_word2vec, propertySchema.nameWithoutDataType()));
            writeAutoInferredNodeFeature(propertySchema);
            return;
        }

        generator.writeStartObject();

        writeFeature(propertySchema, FeatureTypeV2.text_word2vec);

        if (!word2VecSpecification.languages().isEmpty()) {
            generator.writeArrayFieldStart("language");
            for (String language : word2VecSpecification.languages()) {
                generator.writeString(language);
                try {
                    SupportedLanguages.valueOf(language);
                } catch (IllegalArgumentException e) {
                    warnings.add(String.format("Unsupported language for text_word2vec feature for '%s': '%s'. " +
                                    "Supported languages are: %s. " +
                                    "The output embedding is not guaranteed to be valid if you supply another language.",
                            propertySchema.nameWithoutDataType(),
                            language,
                            ErrorMessageHelper.quoteList(Arrays.stream(SupportedLanguages.values()).map(Enum::name).collect(Collectors.toList()))));
                }

            }
            generator.writeEndArray();
        }

        generator.writeEndObject();
    }

    private void writeDatetimeNodeFeature(PropertySchema propertySchema, DatetimeConfigV2 datetimeConfig) throws IOException {
        writeDatetimeNodeFeature(Collections.singletonList(propertySchema), datetimeConfig.datetimeParts());
    }

    private void writeDatetimeNodeFeature(Collection<PropertySchema> propertySchemas, Collection<DatetimePartV2> datetimeParts) throws IOException {

        for (PropertySchema propertySchema : propertySchemas) {

            if (propertySchema.isMultiValue()) {
                warnings.add(String.format("Unable to add datetime feature for '%s'. Multi-value datetime features not currently supported. Adding an auto feature instead.", propertySchema.nameWithoutDataType()));
                writeAutoFeature(Collections.singletonList(propertySchema), ImputerTypeV2.none);
                return;
            }

            generator.writeStartObject();

            writeFeature(propertySchema, FeatureTypeV2.datetime);

            if (!datetimeParts.isEmpty()) {
                generator.writeArrayFieldStart("datetime_parts");
                for (DatetimePartV2 datetimePart : datetimeParts) {
                    generator.writeString(datetimePart.name());
                }
                generator.writeEndArray();
            }

            generator.writeEndObject();
        }
    }

    private void writeNumericalNodeFeature(Collection<PropertySchema> propertySchemas, FeatureOverrideConfigV2 featureOverride) throws IOException {
        writeNumericalNodeFeature(propertySchemas, featureOverride.norm(), featureOverride.imputer(), featureOverride.separator());
    }

    private void writeNumericalNodeFeature(Collection<PropertySchema> propertySchemas, Norm norm, ImputerTypeV2 imputer) throws IOException {
        writeNumericalNodeFeature(propertySchemas, norm, imputer, new Separator());
    }

    private void writeNumericalNodeFeature(Collection<PropertySchema> propertySchemas, Norm norm, ImputerTypeV2 imputer, Separator separator) throws IOException {

        for (PropertySchema propertySchema : propertySchemas) {
            generator.writeStartObject();

            writeFeature(propertySchema, FeatureTypeV2.numerical);

            separator.writeTo(generator, propertySchema.isMultiValue());

            generator.writeStringField("norm", norm.formattedName());

            if (imputer != ImputerTypeV2.none) {
                generator.writeStringField("imputer", imputer.formattedName());
            }

            generator.writeEndObject();
        }
    }

    private void writeCategoricalNodeFeature(Collection<PropertySchema> propertySchemas, FeatureOverrideConfigV2 featureOverride) throws IOException {
        for (PropertySchema propertySchema : propertySchemas) {
            generator.writeStartObject();

            writeFeature(propertySchema, FeatureTypeV2.category);

            featureOverride.separator().writeTo(generator, propertySchema.isMultiValue());

            generator.writeEndObject();
        }
    }

    private void writeAutoFeature(Collection<PropertySchema> propertySchemas, FeatureOverrideConfigV2 featureOverride) throws IOException {
        writeAutoFeature(propertySchemas, featureOverride.imputer(), featureOverride.separator());
    }

    private void writeAutoFeature(Collection<PropertySchema> propertySchemas, ImputerTypeV2 imputer) throws IOException {
        writeAutoFeature(propertySchemas, imputer, new Separator());
    }

    private void writeAutoFeature(Collection<PropertySchema> propertySchemas, ImputerTypeV2 imputer, Separator separator) throws IOException {

        for (PropertySchema propertySchema : propertySchemas) {
            generator.writeStartObject();

            writeFeature(propertySchema, FeatureTypeV2.auto);

            separator.writeTo(generator, propertySchema.isMultiValue());

            if (imputer != ImputerTypeV2.none) {
                generator.writeStringField("imputer", imputer.formattedName());
            }

            generator.writeEndObject();
        }
    }

    private void writeFileName(GraphElementType graphElementType, String outputId) throws IOException {
        generator.writeStringField("file_name", String.format("%s/%s", graphElementType.name(), new File(outputId).getName()));
    }

    private void writeCommaSeparator() throws IOException {
        generator.writeStringField("separator", ",");
    }
}
