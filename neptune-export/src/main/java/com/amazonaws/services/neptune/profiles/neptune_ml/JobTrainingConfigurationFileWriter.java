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

import com.amazonaws.services.neptune.profiles.neptune_ml.parsing.FeatureType;
import com.amazonaws.services.neptune.profiles.neptune_ml.parsing.Norm;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public class JobTrainingConfigurationFileWriter {

    public static Function<PropertySchema, String> COLUMN_NAME_WITH_DATATYPE = PropertySchema::nameWithDataType;
    public static Function<PropertySchema, String> COLUMN_NAME_WITHOUT_DATATYPE = PropertySchema::nameWithoutDataType;

    private final GraphSchema graphSchema;
    private final JsonGenerator generator;
    private final Function<PropertySchema, String> getColumnName;
    private final TrainingJobWriterConfig config;
    private final Collection<String> warnings = new ArrayList<>();

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema,
                                              JsonGenerator generator,
                                              Function<PropertySchema, String> getColumnName) {
        this(graphSchema, generator, getColumnName, new TrainingJobWriterConfig());
    }

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema,
                                              JsonGenerator generator,
                                              Function<PropertySchema, String> getColumnName,
                                              TrainingJobWriterConfig config) {
        this.graphSchema = graphSchema;
        this.generator = generator;
        this.getColumnName = getColumnName;
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

        GraphElementType<Map<String, Object>> graphElementType = GraphElementTypes.Nodes;
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
                writeNodeFeatures(nodeLabel, labelSchema.propertySchemas());
                generator.writeEndObject();
            }
        }

    }

    private void writeNodeLabel(LabelSchema labelSchema, TrainingJobWriterConfig.LabelConfig labelConfig) throws IOException {

        Label label = labelSchema.label();

        if (labelSchema.containsProperty(labelConfig.property())) {
            generator.writeArrayFieldStart("labels");
            PropertySchema propertySchema = labelSchema.getPropertySchema(labelConfig.property());
            generator.writeStartObject();
            generator.writeStringField("label_type", "node");
            generator.writeStringField("sub_label_type", labelConfig.labelType());
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(getColumnName.apply(propertySchema));
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

    private void writeSplitRates(TrainingJobWriterConfig.LabelConfig labelConfig) throws IOException {
        generator.writeArrayFieldStart("split_rate");
        for (Double rate : labelConfig.splitRates()) {
            generator.writeNumber(rate);
        }
        generator.writeEndArray();
    }

    private void writeNodeFeatures(Label label, Collection<PropertySchema> propertySchemas) throws IOException {
        boolean arrayStartHasBeenWritten = false;

        for (PropertySchema propertySchema : propertySchemas) {
            String column = propertySchema.nameWithoutDataType();
            if (!config.isNodeClassificationPropertyForNode(label, column)) {
                if (!arrayStartHasBeenWritten) {
                    generator.writeArrayFieldStart("features");
                    arrayStartHasBeenWritten = true;
                }
                if (config.hasNodeFeatureOverrideForNodeProperty(label, column)) {
                    writeNodeFeatureOverride(label, propertySchema);
                } else {
                    writeNodeFeature(label, propertySchema);
                }
            }
        }

        if (arrayStartHasBeenWritten) {
            generator.writeEndArray();
        }
    }

    private void writeNodeFeature(Label label, PropertySchema propertySchema) throws IOException {

        if (propertySchema.dataType() == DataType.Float ||
                propertySchema.dataType() == DataType.Double) {
            writeNumericalNodeFeature(label, propertySchema, Norm.none);
        }

        if (propertySchema.dataType() == DataType.Byte ||
                propertySchema.dataType() == DataType.Short ||
                propertySchema.dataType() == DataType.Integer ||
                propertySchema.dataType() == DataType.Long) {
            writeNumericalNodeFeature(label, propertySchema, Norm.min_max);
        }

        if (propertySchema.dataType() == DataType.String ||
                propertySchema.dataType() == DataType.Boolean) {
            writeCategoricalNodeFeature(label, propertySchema);
        }
    }

    private void writeNodeFeatureOverride(Label label, PropertySchema propertySchema) throws IOException {
        TrainingJobWriterConfig.FeatureOverrideConfig featureOverride =
                config.getNodeFeatureOverride(label, propertySchema.nameWithoutDataType());
        String featureType = featureOverride.featureType();
        if (FeatureType.category.name().equals(featureType)) {
            writeCategoricalNodeFeature(label, propertySchema);
        } else if (FeatureType.numerical.name().equals(featureType)) {
            writeNumericalNodeFeature(label, propertySchema, featureOverride.norm(), featureOverride.separator());
        }
    }

    private void writeCategoricalNodeFeature(Label label, PropertySchema propertySchema) throws IOException {

        if (config.hasWord2VecSpecification(label, propertySchema.nameWithoutDataType())) {
            writeWord2VecFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "category");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(getColumnName.apply(propertySchema));
            generator.writeEndArray();
            if (propertySchema.isMultiValue()) {
                writeSeparator(";");
            }
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeWord2VecFeature(Label label, PropertySchema propertySchema) throws IOException {
        TrainingJobWriterConfig.Word2VecConfig word2VecConfig =
                config.getWord2VecSpecification(label, propertySchema.nameWithoutDataType());

        generator.writeStartObject();
        generator.writeStringField("feat_type", "node");
        generator.writeStringField("sub_feat_type", "word2vec");
        generator.writeArrayFieldStart("cols");
        generator.writeString("~id");
        generator.writeString(getColumnName.apply(propertySchema));
        generator.writeEndArray();
        generator.writeArrayFieldStart("language");
        for (String language : word2VecConfig.languages()) {
            generator.writeString(language);
        }
        generator.writeEndArray();
        generator.writeStringField("node_type", label.labelsAsString());
        generator.writeEndObject();
    }

    private void writeNumericalNodeFeature(Label label, PropertySchema propertySchema, Norm norm) throws IOException {
        writeNumericalNodeFeature(label, propertySchema, norm, null);
    }

    private void writeNumericalNodeFeature(Label label, PropertySchema propertySchema, Norm norm, String separator) throws IOException {
        if (config.hasNumericalBucketSpecification(label, propertySchema.nameWithoutDataType())) {
            writeNumericalBucketFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            FeatureType.numerical.addTo(generator);
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(getColumnName.apply(propertySchema));
            generator.writeEndArray();
            norm.addTo(generator);
            if (propertySchema.isMultiValue()) {
                writeSeparator(";");
            } else if (StringUtils.isNotEmpty(separator)) {
                writeSeparator(separator);
            }
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeNumericalBucketFeature(Label label, PropertySchema propertySchema) throws IOException {
        TrainingJobWriterConfig.NumericalBucketFeatureConfig featureConfig =
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
            generator.writeString(getColumnName.apply(propertySchema));
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
        GraphElementType<Map<String, Object>> graphElementType = GraphElementTypes.Edges;
        GraphElementSchemas graphElementSchemas = graphSchema.graphElementSchemasFor(graphElementType);

        for (Label edgeLabel : graphElementSchemas.labels()) {
            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(edgeLabel);
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(edgeLabel);
            for (String outputId : outputIds) {
                generator.writeStartObject();
                writeFileName(graphElementType, outputId);
                writeSeparator(",");

                generator.writeArrayFieldStart("edges");
                generator.writeStartObject();
                writeEdgeSpecType();
                writeCols();
                writeEdgeType(edgeLabel);
                generator.writeEndObject();
                generator.writeEndArray();

                if (config.hasEdgeClassificationSpecificationForEdge(edgeLabel)) {
                    writeEdgeLabel(labelSchema, config.getEdgeClassificationPropertyForEdge(edgeLabel));
                }
                writeEdgeFeatures(edgeLabel, labelSchema.propertySchemas());

                generator.writeEndObject();
            }
        }
    }

    private void writeEdgeFeatures(Label label, Collection<PropertySchema> propertySchemas) throws IOException {
        boolean arrayStartHasBeenWritten = false;

        for (PropertySchema propertySchema : propertySchemas) {
            if (!config.isEdgeClassificationPropertyForEdge(label, propertySchema.nameWithoutDataType())) {

                if (!arrayStartHasBeenWritten) {
                    generator.writeArrayFieldStart("features");
                    arrayStartHasBeenWritten = true;
                }

                if (!propertySchema.isMultiValue()) {
                    if (config.hasEdgeFeatureOverrideForEdgeProperty(label, propertySchema.nameWithoutDataType())) {
                        writeEdgeFeatureOverride(label, propertySchema);
                    } else {
                        writeNumericalEdgeFeature(label, propertySchema, Norm.min_max);
                    }
                }
            }
        }
        if (arrayStartHasBeenWritten) {
            generator.writeEndArray();
        }
    }

    private void writeEdgeFeatureOverride(Label label, PropertySchema propertySchema) throws IOException {

        TrainingJobWriterConfig.FeatureOverrideConfig featureOverride =
                config.getEdgeFeatureOverride(label, propertySchema.nameWithoutDataType());
        String featureType = featureOverride.featureType();
        if (FeatureType.numerical.name().equals(featureType)) {
            writeNumericalEdgeFeature(label, propertySchema, featureOverride.norm(), featureOverride.separator());
        }
    }

    private void writeNumericalEdgeFeature(Label label, PropertySchema propertySchema, Norm norm) throws IOException {
        writeNumericalEdgeFeature(label, propertySchema, norm, null);
    }

    private void writeNumericalEdgeFeature(Label label, PropertySchema propertySchema, Norm norm, String separator) throws IOException {

        generator.writeStartObject();
        generator.writeStringField("feat_type", "edge");
        FeatureType.numerical.addTo(generator);
        generator.writeArrayFieldStart("cols");
        generator.writeString("~from");
        generator.writeString("~to");
        generator.writeString(getColumnName.apply(propertySchema));
        generator.writeEndArray();
        norm.addTo(generator);
        if (propertySchema.isMultiValue()) {
            writeSeparator(";");
        } else if (StringUtils.isNotEmpty(separator)) {
            writeSeparator(separator);
        }
        writeEdgeType(label);
        generator.writeEndObject();

    }

    private void writeEdgeLabel(LabelSchema labelSchema, TrainingJobWriterConfig.LabelConfig labelConfig) throws IOException {

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
            generator.writeString(getColumnName.apply(propertySchema));
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

    private void writeFileName(GraphElementType<Map<String, Object>> graphElementType, String outputId) throws IOException {
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
