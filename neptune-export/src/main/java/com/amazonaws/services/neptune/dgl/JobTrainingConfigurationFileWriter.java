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

package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.core.JsonGenerator;

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
    private final TrainingJobConfig config;
    private final Collection<String> warnings = new ArrayList<>();

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema,
                                              JsonGenerator generator,
                                              Function<PropertySchema, String> getColumnName) {
        this(graphSchema, generator, getColumnName, new TrainingJobConfig());
    }

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema,
                                              JsonGenerator generator,
                                              Function<PropertySchema, String> getColumnName,
                                              TrainingJobConfig config) {
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
                if (config.hasNodeClassificationSpecificationForNodeType(nodeLabel)) {
                    writeNodeClassLabel(labelSchema, config.getNodeClassificationColumnForNodeType(nodeLabel));
                } else {
                    writeNodeFeatures(nodeLabel, labelSchema.propertySchemas());
                }
                generator.writeEndObject();
            }
        }

    }

    private void writeNodeClassLabel(LabelSchema labelSchema, String column) throws IOException {

        Label label = labelSchema.label();

        if (labelSchema.containsProperty(column)) {
            generator.writeArrayFieldStart("labels");
            PropertySchema propertySchema = labelSchema.getPropertySchema(column);
            generator.writeStartObject();
            generator.writeStringField("label_type", "node");
            generator.writeStringField("sub_label_type", "node_class_label");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(column);
            generator.writeEndArray();
            writeSplitRates();
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
                            column));
        }
    }

    private void writeSplitRates() throws IOException {
        generator.writeArrayFieldStart("split_rate");
        for (Double rate : config.splitRates()) {
            generator.writeNumber(rate);
        }
        generator.writeEndArray();
    }

    private void writeNodeFeatures(Label label, Collection<PropertySchema> propertySchemas) throws IOException {
        generator.writeArrayFieldStart("features");
        for (PropertySchema propertySchema : propertySchemas) {
            if (propertySchema.dataType() == DataType.Float ||
                    propertySchema.dataType() == DataType.Double) {
                writeNumericalNodeFeatureForFloat(label, propertySchema);
            }
            if (propertySchema.dataType() == DataType.Byte ||
                    propertySchema.dataType() == DataType.Short ||
                    propertySchema.dataType() == DataType.Integer ||
                    propertySchema.dataType() == DataType.Long) {
                writeNumericalNodeFeatureForInt(label, propertySchema);
            }
            if (propertySchema.dataType() == DataType.String) {
                writeCategoricalNodeFeatureForString(label, propertySchema);
            }
        }
        generator.writeEndArray();
    }

    private void writeCategoricalNodeFeatureForString(Label label, PropertySchema propertySchema) throws IOException {

        if (config.hasWord2VecSpecificationForNodeTypeAndColumn(label, propertySchema.nameWithoutDataType())) {
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
        TrainingJobConfig.Word2VecConfig word2VecConfig =
                config.getWord2VecSpecificationForNodeType(label, propertySchema.nameWithoutDataType());

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

    private void writeNumericalNodeFeatureForFloat(Label label, PropertySchema propertySchema) throws IOException {

        if (config.hasNumericalBucketSpecificationForNodeType(label, propertySchema.nameWithoutDataType())) {
            writeNumericalBucketFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "numerical");
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

    private void writeNumericalNodeFeatureForInt(Label label, PropertySchema propertySchema) throws IOException {
        if (config.hasNumericalBucketSpecificationForNodeType(label, propertySchema.nameWithoutDataType())) {
            writeNumericalBucketFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "numerical");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(getColumnName.apply(propertySchema));
            generator.writeEndArray();
            generator.writeStringField("norm", "min-max");
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeNumericalBucketFeature(Label label, PropertySchema propertySchema) throws IOException {
        TrainingJobConfig.NumericalBucketFeatureConfig featureConfig =
                config.getNumericalBucketSpecificationForNodeType(label, propertySchema.nameWithoutDataType());

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
            generator.writeObject(featureConfig.low());
            generator.writeObject(featureConfig.high());
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

                if (config.hasEdgeClassificationSpecificationForEdgeType(edgeLabel)) {
                    writeEdgeClassLabel(labelSchema, config.getEdgeClassificationColumnForEdgeType(edgeLabel));
                } else {
                    writeEdgeFeatures(edgeLabel, labelSchema.propertySchemas());
                }

                generator.writeEndObject();
            }
        }
    }

    private void writeEdgeFeatures(Label label, Collection<PropertySchema> propertySchemas) throws IOException {
        generator.writeArrayFieldStart("features");
        for (PropertySchema propertySchema : propertySchemas) {
            writeNumericalEdgeFeature(label, propertySchema);
        }
        generator.writeEndArray();
    }

    private void writeNumericalEdgeFeature(Label label, PropertySchema propertySchema) throws IOException {
        if (!propertySchema.isMultiValue()) {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "edge");
            generator.writeStringField("sub_feat_type", "numerical");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~from");
            generator.writeString("~to");
            generator.writeString(getColumnName.apply(propertySchema));
            generator.writeEndArray();
            generator.writeStringField("norm", "min-max");
            writeEdgeType(label);
            generator.writeEndObject();
        }
    }

    private void writeEdgeClassLabel(LabelSchema labelSchema, String column) throws IOException {

        Label label = labelSchema.label();
        if (labelSchema.containsProperty(column)) {
            generator.writeArrayFieldStart("labels");
            generator.writeStartObject();
            generator.writeStringField("label_type", "edge");
            generator.writeStringField("sub_label_type", "edge_class_label");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~from");
            generator.writeString("~to");
            generator.writeString(column);
            generator.writeEndArray();
            writeSplitRates();
            if (labelSchema.getPropertySchema(column).isMultiValue()){
                writeSeparator(";");
            }
            writeEdgeType(label);
            generator.writeEndObject();
            generator.writeEndArray();
        } else {
            warnings.add(
                    String.format("Unable to add edge class label: Edge of type '%s' does not contain property '%s'.",
                            label.labelsAsString(),
                            column));
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