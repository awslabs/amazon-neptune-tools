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

package com.amazonaws.services.neptune.profiles.neptune_ml.v1;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.PropertyName;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Norm;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.*;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PropertyGraphTrainingDataConfigWriterV1 {

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
    private final TrainingDataWriterConfigV1 config;
    private final PrinterOptions printerOptions;
    private final Collection<String> warnings = new ArrayList<>();

    public PropertyGraphTrainingDataConfigWriterV1(GraphSchema graphSchema,
                                                   JsonGenerator generator,
                                                   PropertyName propertyName,
                                                   PrinterOptions printerOptions) {
        this(graphSchema, generator, propertyName, printerOptions, new TrainingDataWriterConfigV1());
    }

    public PropertyGraphTrainingDataConfigWriterV1(GraphSchema graphSchema,
                                                   JsonGenerator generator,
                                                   PropertyName propertyName,
                                                   PrinterOptions printerOptions,
                                                   TrainingDataWriterConfigV1 config
    ) {
        this.graphSchema = graphSchema;
        this.generator = generator;
        this.propertyName = propertyName;
        this.printerOptions = printerOptions;
        this.config = config;
    }

    public void write() throws IOException {

        generator.writeStartObject();

        generator.writeArrayFieldStart("graph");
        writeNodes();
        writeEdges();
        generator.writeEndArray();

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

        for (Label nodeLabel : graphElementSchemas.labels()) {
            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(nodeLabel);
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(nodeLabel);
            for (String outputId : outputIds) {
                generator.writeStartObject();
                writeFileName(graphElementType, outputId);
                writeSeparator(",");
                if (config.hasNodeClassificationSpecificationForNode(nodeLabel)) {
                    writeNodeLabel(labelSchema, config.getNodeClassificationPropertyForNode(nodeLabel));
                }
                writeNodeFeatures(nodeLabel, labelSchema.propertySchemas(), labelSchema);
                generator.writeEndObject();
            }
        }

    }

    private void writeNodeLabel(LabelSchema labelSchema, LabelConfigV1 labelConfig) throws IOException {

        Label label = labelSchema.label();

        if (labelSchema.containsProperty(labelConfig.property())) {
            generator.writeArrayFieldStart("labels");
            PropertySchema propertySchema = labelSchema.getPropertySchema(labelConfig.property());
            generator.writeStartObject();
            generator.writeStringField("label_type", "node");
            generator.writeStringField("sub_label_type", labelConfig.labelType());
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(propertyName.escaped(propertySchema, printerOptions));
            generator.writeEndArray();
            writeSplitRates(labelConfig);
            if (propertySchema.isMultiValue()) {
                writeSeparator(";");
            }
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
            generator.writeEndArray();
        } else {
            warnings.add(
                    String.format("Unable to add node class label: Node of type '%s' does not contain property '%s'.",
                            label.fullyQualifiedLabel(),
                            labelConfig.property()));
        }
    }

    private void writeSplitRates(LabelConfigV1 labelConfig) throws IOException {
        generator.writeArrayFieldStart("split_rate");
        for (Double rate : labelConfig.splitRates()) {
            generator.writeNumber(rate);
        }
        generator.writeEndArray();
    }

    private void writeNodeFeatures(Label label, Collection<PropertySchema> propertySchemas, LabelSchema labelSchema) throws IOException {
        boolean arrayStartHasBeenWritten = false;

        for (PropertySchema propertySchema : propertySchemas) {
            String column = propertySchema.nameWithoutDataType();
            if (!config.isNodeClassificationPropertyForNode(label, column)) {
                if (!arrayStartHasBeenWritten) {
                    generator.writeArrayFieldStart("features");
                    arrayStartHasBeenWritten = true;
                }
                if (!config.hasNodeFeatureOverrideForNodeProperty(label, column)) {
                    writeNodeFeature(label, propertySchema, labelSchema);
                }
            }
        }

        for (FeatureOverrideConfigV1 featureOverride : config.getNodeFeatureOverrides(label)) {
            writeNodeFeatureOverride(label, featureOverride, propertySchemas, labelSchema);
        }

        if (arrayStartHasBeenWritten) {
            generator.writeEndArray();
        }
    }

    private void writeNodeFeature(Label label, PropertySchema propertySchema, LabelSchema labelSchema) throws IOException {

        if (propertySchema.dataType() == DataType.Float ||
                propertySchema.dataType() == DataType.Double) {
            writeNumericalNodeFeature(label, Collections.singletonList(propertySchema), Norm.min_max, labelSchema);
        }

        if (propertySchema.dataType() == DataType.Byte ||
                propertySchema.dataType() == DataType.Short ||
                propertySchema.dataType() == DataType.Integer ||
                propertySchema.dataType() == DataType.Long) {
            writeNumericalNodeFeature(label, Collections.singletonList(propertySchema), Norm.min_max, labelSchema);
        }

        if (propertySchema.dataType() == DataType.String ||
                propertySchema.dataType() == DataType.Boolean) {
            writeCategoricalNodeFeature(label, Collections.singletonList(propertySchema));
        }
    }

    private void writeNodeFeatureOverride(Label label,
                                          FeatureOverrideConfigV1 featureOverride,
                                          Collection<PropertySchema> propertySchemas,
                                          LabelSchema labelSchema) throws IOException {

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
                FeatureTypeV1 featureType = featureOverride.featureType();
                if (FeatureTypeV1.category == featureType) {
                    writeCategoricalNodeFeature(label, Collections.singletonList(propertySchema), featureOverride.separator());
                } else if (FeatureTypeV1.numerical == featureType) {
                    writeNumericalNodeFeature(label, Collections.singletonList(propertySchema), featureOverride.norm(), labelSchema, featureOverride.separator());
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
                FeatureTypeV1 featureType = featureOverride.featureType();
                List<PropertySchema> multiPropertySchemas = propertySchemas.stream()
                        .filter(p -> featureOverride.properties().contains(p.nameWithoutDataType()))
                        .collect(Collectors.toList());
                if (FeatureTypeV1.category == featureType) {
                    writeCategoricalNodeFeature(label, multiPropertySchemas);
                } else if (FeatureTypeV1.numerical == featureType) {
                    writeNumericalNodeFeature(label, multiPropertySchemas, featureOverride.norm(), labelSchema);
                }
            }
        }

    }

    private void writeCategoricalNodeFeature(Label label, Collection<PropertySchema> propertySchemas) throws IOException {
        writeCategoricalNodeFeature(label, propertySchemas, new Separator());
    }

    private void writeCategoricalNodeFeature(Label label, Collection<PropertySchema> propertySchemas, Separator separator) throws IOException {

        boolean isSinglePropertyFeature = propertySchemas.size() == 1;
        PropertySchema firstPropertySchema = propertySchemas.iterator().next();

        if (isSinglePropertyFeature && config.hasWord2VecSpecification(label, firstPropertySchema.nameWithoutDataType())) {
            writeWord2VecFeature(label, firstPropertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "category");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            for (PropertySchema propertySchema : propertySchemas) {
                generator.writeString(propertyName.escaped(propertySchema, printerOptions));
            }
            generator.writeEndArray();
            if (isSinglePropertyFeature) {
                separator.writeTo(generator, firstPropertySchema.isMultiValue());
            }
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeWord2VecFeature(Label label, PropertySchema propertySchema) throws IOException {
        Word2VecConfig word2VecConfig =
                config.getWord2VecSpecification(label, propertySchema.nameWithoutDataType());

        generator.writeStartObject();
        generator.writeStringField("feat_type", "node");
        generator.writeStringField("sub_feat_type", "word2vec");
        generator.writeArrayFieldStart("cols");
        generator.writeString("~id");
        generator.writeString(propertyName.escaped(propertySchema, printerOptions));
        generator.writeEndArray();
        generator.writeArrayFieldStart("language");
        for (String language : word2VecConfig.languages()) {
            generator.writeString(language);
        }
        generator.writeEndArray();
        generator.writeStringField("node_type", label.labelsAsString());
        generator.writeEndObject();
    }

    private void writeNumericalNodeFeature(Label label, Collection<PropertySchema> propertySchemas, Norm norm, LabelSchema labelSchema) throws IOException {
        writeNumericalNodeFeature(label, propertySchemas, norm, labelSchema, null);
    }

    private void writeNumericalNodeFeature(Label label, Collection<PropertySchema> propertySchemas, Norm norm, LabelSchema labelSchema, Separator separator) throws IOException {

        boolean isSinglePropertyFeature = propertySchemas.size() == 1;
        PropertySchema firstPropertySchema = propertySchemas.iterator().next();

        if (isSinglePropertyFeature && config.hasNumericalBucketSpecification(label, firstPropertySchema.nameWithoutDataType())) {
            writeNumericalBucketFeature(label, firstPropertySchema);
        } else {

            List<String> multiValueProperties = propertySchemas.stream()
                    .filter(PropertySchema::isMultiValue)
                    .map(PropertySchema::nameWithoutDataType)
                    .collect(Collectors.toList());

            if (!multiValueProperties.isEmpty()) {
                warnings.add(String.format("Unable to add numerical node feature: Node of type '%s' has one or more multi-value numerical properties: %s.",
                        label.fullyQualifiedLabel(),
                        multiValueProperties));
                return;
            }

            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            FeatureTypeV1.numerical.addTo(generator);
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            for (PropertySchema propertySchema : propertySchemas) {
                generator.writeString(propertyName.escaped(propertySchema, printerOptions));
            }
            generator.writeEndArray();
            norm.addTo(generator);

            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeNumericalBucketFeature(Label label, PropertySchema propertySchema) throws IOException {
        NumericalBucketFeatureConfigV1 featureConfig =
                config.getNumericalBucketSpecification(label, propertySchema.nameWithoutDataType());

        if (propertySchema.isMultiValue()) {
            warnings.add(String.format(
                    "Unable to add numerical bucket feature: Property '%s' of node type '%s' is a multi-value property.",
                    propertySchema.nameWithoutDataType(),
                    label.fullyQualifiedLabel()));
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "bucket_numerical");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(propertyName.escaped(propertySchema, printerOptions));
            generator.writeEndArray();
            generator.writeArrayFieldStart("range");
            generator.writeObject(featureConfig.range().low());
            generator.writeObject(featureConfig.range().high());
            generator.writeEndArray();
            generator.writeNumberField("bucket_cnt", featureConfig.bucketCount());
            generator.writeNumberField("slide_window_size", featureConfig.slideWindowSize());
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeEdges() throws IOException {
        GraphElementType graphElementType = GraphElementType.edges;
        GraphElementSchemas graphElementSchemas = graphSchema.graphElementSchemasFor(graphElementType);

        for (Label edgeLabel : graphElementSchemas.labels()) {
            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(edgeLabel);
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(edgeLabel);
            for (String outputId : outputIds) {
                generator.writeStartObject();
                writeFileName(graphElementType, outputId);
                writeSeparator(",");

                if (graphElementSchemas.getSchemaFor(edgeLabel).propertyCount() == 0) {

                    generator.writeArrayFieldStart("edges");
                    generator.writeStartObject();
                    writeEdgeSpecType();
                    writeCols();
                    writeEdgeType(edgeLabel);
                    generator.writeEndObject();
                    generator.writeEndArray();
                } else {
                    if (config.hasEdgeClassificationSpecificationForEdge(edgeLabel)) {
                        writeEdgeLabel(labelSchema, config.getEdgeClassificationPropertyForEdge(edgeLabel));
                    }
                    writeEdgeFeatures(edgeLabel, labelSchema.propertySchemas(), labelSchema);
                }

                generator.writeEndObject();
            }
        }
    }

    private void writeEdgeFeatures(Label label, Collection<PropertySchema> propertySchemas, LabelSchema labelSchema) throws IOException {
        boolean arrayStartHasBeenWritten = false;

        for (PropertySchema propertySchema : propertySchemas) {
            if (!config.isEdgeClassificationPropertyForEdge(label, propertySchema.nameWithoutDataType())) {

                if (!arrayStartHasBeenWritten) {
                    generator.writeArrayFieldStart("features");
                    arrayStartHasBeenWritten = true;
                }

                if (!propertySchema.isMultiValue()) {
                    if (!config.hasEdgeFeatureOverrideForEdgeProperty(label, propertySchema.nameWithoutDataType())) {
                        writeNumericalEdgeFeature(label, Collections.singletonList(propertySchema), Norm.min_max, labelSchema);
                    }
                }
            }
        }

        for (FeatureOverrideConfigV1 featureOverride : config.getEdgeFeatureOverrides(label)) {
            writeEdgeFeatureOverride(label, featureOverride, propertySchemas, labelSchema);
        }

        if (arrayStartHasBeenWritten) {
            generator.writeEndArray();
        }
    }

    private void writeEdgeFeatureOverride(Label label,
                                          FeatureOverrideConfigV1 featureOverride,
                                          Collection<PropertySchema> propertySchemas,
                                          LabelSchema labelSchema) throws IOException {

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
                FeatureTypeV1 featureType = featureOverride.featureType();
                if (FeatureTypeV1.numerical == featureType) {
                    writeNumericalEdgeFeature(label, Collections.singletonList(propertySchema), featureOverride.norm(), labelSchema, featureOverride.separator());
                }
            }
        } else {
            boolean allPropertiesPresent = featureOverride.properties().stream()
                    .allMatch(p ->
                            propertySchemas.stream()
                                    .anyMatch(s -> s.nameWithoutDataType().equals(p)));
            if (!allPropertiesPresent) {
                warnings.add(String.format("Unable to add multi-property edge feature: Edge of type '%s' does not contain one or more of the following properties: %s.",
                        label.fullyQualifiedLabel(),
                        featureOverride.properties().stream()
                                .map(s -> String.format("'%s'", s))
                                .collect(Collectors.joining(", "))));
            } else {
                FeatureTypeV1 featureType = featureOverride.featureType();
                List<PropertySchema> multiPropertySchemas = propertySchemas.stream()
                        .filter(p -> featureOverride.properties().contains(p.nameWithoutDataType()))
                        .collect(Collectors.toList());
                if (FeatureTypeV1.numerical == featureType) {
                    writeNumericalEdgeFeature(label, multiPropertySchemas, featureOverride.norm(), labelSchema);
                }
            }
        }

    }

    private void writeNumericalEdgeFeature(Label label, Collection<PropertySchema> propertySchemas, Norm norm, LabelSchema labelSchema) throws IOException {
        writeNumericalEdgeFeature(label, propertySchemas, norm, labelSchema, new Separator());
    }

    private void writeNumericalEdgeFeature(Label label, Collection<PropertySchema> propertySchemas, Norm norm, LabelSchema labelSchema, Separator separator) throws IOException {

        boolean isSinglePropertyFeature = propertySchemas.size() == 1;
        PropertySchema firstPropertySchema = propertySchemas.iterator().next();

        if (isSinglePropertyFeature) {
            PropertySchemaStats propertySchemaStats = labelSchema.getPropertySchemaStats(firstPropertySchema.property());
            if (firstPropertySchema.isMultiValue() && !propertySchemaStats.isUniformMultiValueSize()) {
                warnings.add(String.format("Unable to add numerical edge feature: Edge of type '%s' has a multi-value numerical property '%s' with differing numbers of values.",
                        label.fullyQualifiedLabel(),
                        firstPropertySchema.property()));
                return;
            }
        }

        generator.writeStartObject();
        generator.writeStringField("feat_type", "edge");
        FeatureTypeV1.numerical.addTo(generator);
        generator.writeArrayFieldStart("cols");
        generator.writeString("~from");
        generator.writeString("~to");
        for (PropertySchema propertySchema : propertySchemas) {
            generator.writeString(propertyName.escaped(propertySchema, printerOptions));
        }
        generator.writeEndArray();
        norm.addTo(generator);
        if (isSinglePropertyFeature) {
            separator.writeTo(generator, firstPropertySchema.isMultiValue());
        }
        writeEdgeType(label);
        generator.writeEndObject();

    }

    private void writeEdgeLabel(LabelSchema labelSchema, LabelConfigV1 labelConfig) throws IOException {

        Label label = labelSchema.label();
        if (labelSchema.containsProperty(labelConfig.property())) {
            PropertySchema propertySchema = labelSchema.getPropertySchema(labelConfig.property());
            generator.writeArrayFieldStart("labels");
            generator.writeStartObject();
            generator.writeStringField("label_type", "edge");
            generator.writeStringField("sub_label_type", labelConfig.labelType());
            generator.writeArrayFieldStart("cols");
            generator.writeString("~from");
            generator.writeString("~to");
            generator.writeString(propertyName.escaped(propertySchema, printerOptions));
            generator.writeEndArray();
            writeSplitRates(labelConfig);
            if (propertySchema.isMultiValue()) {
                writeSeparator(";");
            }
            writeEdgeType(label);
            generator.writeEndObject();
            generator.writeEndArray();
        } else {
            warnings.add(
                    String.format("Unable to add edge class label: Edge of type '%s' does not contain property '%s'.",
                            label.labelsAsString(),
                            labelConfig.property()));
        }
    }

    private void writeSeparator(String separator) throws IOException {
        generator.writeStringField("separator", separator);
    }

    private void writeFileName(GraphElementType graphElementType, String outputId) throws IOException {
        generator.writeStringField("file_name", String.format("%s/%s", graphElementType.name(), new File(outputId).getName()));
    }

    private void writeEdgeSpecType() throws IOException {
        generator.writeStringField("edge_spec_type", "edge");
    }

    private void writeCols() throws IOException {
        generator.writeArrayFieldStart("cols");
        generator.writeString("~from");
        generator.writeString("~to");
        generator.writeEndArray();
    }

    private void writeEdgeType(Label label) throws IOException {
        generator.writeArrayFieldStart("edge_type");
        generator.writeString(label.fromLabelsAsString());
        generator.writeString(label.labelsAsString());
        generator.writeString(label.toLabelsAsString());
        generator.writeEndArray();
    }
}
