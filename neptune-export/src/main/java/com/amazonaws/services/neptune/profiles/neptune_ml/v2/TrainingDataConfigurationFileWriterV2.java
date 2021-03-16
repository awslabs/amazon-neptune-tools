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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TrainingDataConfigurationFileWriterV2 {

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

    public TrainingDataConfigurationFileWriterV2(GraphSchema graphSchema,
                                                 JsonGenerator generator,
                                                 PropertyName propertyName,
                                                 PrinterOptions printerOptions) {
        this(graphSchema, generator, propertyName, printerOptions, new TrainingDataWriterConfigV2());
    }

    public TrainingDataConfigurationFileWriterV2(GraphSchema graphSchema,
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

        GraphElementType<Map<String, Object>> graphElementType = GraphElementTypes.Nodes;
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

//                if (config.hasNodeClassificationSpecificationForNode(nodeLabel)) {
//                    writeNodeLabel(labelSchema, config.getNodeClassificationPropertyForNode(nodeLabel));
//                }
                generator.writeEndObject();
            }
        }

        generator.writeEndArray();
    }


    private void writeEdges() throws IOException {

        GraphElementType<Map<String, Object>> graphElementType = GraphElementTypes.Edges;
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
                writeEdgeFeatures(labelSchema);

//                if (config.hasNodeClassificationSpecificationForNode(nodeLabel)) {
//                    writeNodeLabel(labelSchema, config.getNodeClassificationPropertyForNode(nodeLabel));
//                }
                generator.writeEndObject();
            }
        }

        generator.writeEndArray();
    }

    private void writeNodeType(LabelSchema labelSchema) throws IOException {
        generator.writeArrayFieldStart("node_type");

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

    private void writeEdgeFeatures(LabelSchema labelSchema) throws IOException {

        Label label = labelSchema.label();
        Collection<PropertySchema> propertySchemas = labelSchema.propertySchemas();

        generator.writeArrayFieldStart("features");

        for (PropertySchema propertySchema : propertySchemas) {
            String column = propertySchema.nameWithoutDataType();
            if (config.allowAutoInferEdgeFeature(label, column)) {
                writeAutoInferredEdgeFeature(propertySchema);
            }
        }

        for (FeatureOverrideConfigV2 featureOverride : config.getEdgeFeatureOverrides(label)) {
            writeEdgeFeatureOverride(labelSchema, featureOverride);
        }

        generator.writeEndArray();
    }

    private void writeNodeFeatureOverride(LabelSchema labelSchema, FeatureOverrideConfigV2 featureOverride) throws IOException {

        Label label = labelSchema.label();
        Collection<PropertySchema> propertySchemas = labelSchema.propertySchemas();

        if (featureOverride.isSinglePropertyOverride()) {
            PropertySchema propertySchema = propertySchemas.stream()
                    .filter(p -> p.nameWithoutDataType().equals(featureOverride.firstProperty()))
                    .findFirst()
                    .orElse(null);
            if (propertySchema == null) {
                warnings.add(String.format("Unable to add node feature: Node of type '%s' does not contain property '%s'.",
                        label.fullyQualifiedLabel(),
                        featureOverride.firstProperty()));
            } else {
                FeatureTypeV2 featureType = featureOverride.featureType();
                if (FeatureTypeV2.category == featureType) {
                    writeCategoricalNodeFeature(Collections.singletonList(propertySchema), featureOverride);
                } else if (FeatureTypeV2.numerical == featureType) {
                    writeNumericalNodeFeature(Collections.singletonList(propertySchema), featureOverride);
                } else if (FeatureTypeV2.auto == featureType){
                    writeAutoFeature(Collections.singletonList(propertySchema), featureOverride);
                } else {
                    warnings.add(String.format("Unsupported feature type override for node: %s.", featureType.name()));
                }
            }
        } else {
            boolean allPropertiesPresent = featureOverride.properties().stream()
                    .allMatch(p ->
                            propertySchemas.stream()
                                    .anyMatch(s -> s.nameWithoutDataType().equals(p)));
            if (!allPropertiesPresent) {
                warnings.add(String.format("Unable to add multi-property node feature: Node of type '%s' does not contain one or more of the following properties: %s.",
                        label.fullyQualifiedLabel(),
                        featureOverride.properties().stream()
                                .map(s -> String.format("'%s'", s))
                                .collect(Collectors.joining(", "))));
            } else {
                FeatureTypeV2 featureType = featureOverride.featureType();
                List<PropertySchema> multiPropertySchemas = propertySchemas.stream()
                        .filter(p -> featureOverride.properties().contains(p.nameWithoutDataType()))
                        .collect(Collectors.toList());
                if (FeatureTypeV2.category == featureType) {
                    writeCategoricalNodeFeature(multiPropertySchemas, featureOverride);
                } else if (FeatureTypeV2.numerical == featureType) {
                    writeNumericalNodeFeature(multiPropertySchemas, featureOverride);
                }else if (FeatureTypeV2.auto == featureType){
                    writeAutoInferredNodeFeature(multiPropertySchemas);
                } else {
                    warnings.add(String.format("Unsupported multi-property feature type override for node: %s.", featureType.name()));
                }
            }
        }
    }

    private void writeEdgeFeatureOverride(LabelSchema labelSchema, FeatureOverrideConfigV2 featureOverride) throws IOException {

        Label label = labelSchema.label();
        Collection<PropertySchema> propertySchemas = labelSchema.propertySchemas();

        if (featureOverride.isSinglePropertyOverride()) {
            PropertySchema propertySchema = propertySchemas.stream()
                    .filter(p -> p.nameWithoutDataType().equals(featureOverride.firstProperty()))
                    .findFirst()
                    .orElse(null);
            if (propertySchema == null) {
                warnings.add(String.format("Unable to add edge feature: Edge of type '%s' does not contain property '%s'.",
                        label.fullyQualifiedLabel(),
                        featureOverride.firstProperty()));
            } else {
                FeatureTypeV2 featureType = featureOverride.featureType();
                if (FeatureTypeV2.auto == featureType) {
                    writeAutoFeature(Collections.singletonList(propertySchema), featureOverride);
                } else if (FeatureTypeV2.numerical == featureType) {
                    writeNumericalEdgeFeature(Collections.singletonList(propertySchema), featureOverride);
                } else {
                    warnings.add(String.format("Unsupported feature type override for edge: %s.", featureType.name()));
                }
            }
        } else {
            boolean allPropertiesPresent = featureOverride.properties().stream()
                    .allMatch(p ->
                            propertySchemas.stream()
                                    .anyMatch(s -> s.nameWithoutDataType().equals(p)));
            if (!allPropertiesPresent) {
                warnings.add(String.format("Unable to add multi-property edge feature: Node of type '%s' does not contain one or more of the following properties: %s.",
                        label.fullyQualifiedLabel(),
                        featureOverride.properties().stream()
                                .map(s -> String.format("'%s'", s))
                                .collect(Collectors.joining(", "))));
            } else {
                FeatureTypeV2 featureType = featureOverride.featureType();
                List<PropertySchema> multiPropertySchemas = propertySchemas.stream()
                        .filter(p -> featureOverride.properties().contains(p.nameWithoutDataType()))
                        .collect(Collectors.toList());
                if (FeatureTypeV2.numerical == featureType) {
                    writeNumericalEdgeFeature(multiPropertySchemas, featureOverride);
                }else if (FeatureTypeV2.auto == featureType){
                    writeAutoInferredEdgeFeature(multiPropertySchemas);
                } else {
                    warnings.add(String.format("Unsupported multi-property feature type override for edge: %s.", featureType.name()));
                }
            }
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

    private void writeAutoInferredEdgeFeature(Collection<PropertySchema> propertySchemas) throws IOException {
        for (PropertySchema propertySchema : propertySchemas) {
            writeAutoFeature(Collections.singletonList(propertySchema), ImputerTypeV2.none);
        }
    }

    private void writeAutoInferredEdgeFeature(PropertySchema propertySchema) throws IOException {
        writeAutoFeature(Collections.singletonList(propertySchema), ImputerTypeV2.none);
    }


    private void writeFeature(PropertySchema propertySchema, FeatureTypeV2 featureType) throws IOException {
        generator.writeArrayFieldStart("feature");
        generator.writeString(propertyName.escaped(propertySchema, printerOptions)); // column
        generator.writeString(propertyName.escaped(propertySchema, printerOptions)); // feature name
        generator.writeString(featureType.name());
        generator.writeEndArray();
    }

    private void writeTfIdfNodeFeature(PropertySchema propertySchema, TfIdfConfigV2 tfIdfSpecification) throws IOException {

        if (propertySchema.isMultiValue()){
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

        if (propertySchema.isMultiValue()){
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

        if (propertySchema.isMultiValue()){
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
                } catch (IllegalArgumentException e){
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

    private void writeNumericalEdgeFeature(Collection<PropertySchema> propertySchemas, FeatureOverrideConfigV2 featureOverride) throws IOException {

        for (PropertySchema propertySchema : propertySchemas) {

            generator.writeStartObject();

            writeFeature(propertySchema, FeatureTypeV2.numerical);

            generator.writeStringField("norm", featureOverride.norm().formattedName());

            if (featureOverride.imputer() != ImputerTypeV2.none) {
                generator.writeStringField("imputer", featureOverride.imputer().formattedName());
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

    private void writeFileName(GraphElementType<Map<String, Object>> graphElementType, String outputId) throws IOException {
        generator.writeStringField("file_name", String.format("%s/%s", graphElementType.name(), new File(outputId).getName()));
    }

    private void writeCommaSeparator() throws IOException {
        generator.writeStringField("separator", ",");
    }
}
