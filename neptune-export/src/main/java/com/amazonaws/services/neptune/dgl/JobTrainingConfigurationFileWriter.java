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

public class JobTrainingConfigurationFileWriter {

    private final GraphSchema graphSchema;
    private final JsonGenerator generator;
    private final TrainingJobConfig config;
    private final Collection<String> warnings = new ArrayList<>();

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema, JsonGenerator generator) {
        this(graphSchema, generator, new TrainingJobConfig());
    }

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema,
                                              JsonGenerator generator,
                                              TrainingJobConfig config) {
        this.graphSchema = graphSchema;
        this.generator = generator;
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
                writeSeparator();
                if (config.hasNodeClassificationSpecificationForNodeType(nodeLabel)) {
                    writeNodeClassLabel(labelSchema, config.getNodeClassificationColumnForNodeType(nodeLabel));
                } else {
                    writeFeatures(nodeLabel, labelSchema.propertySchemas());
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
                generator.writeStringField("separator", ";");
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

    private void writeFeatures(Label label, Collection<PropertySchema> propertySchemas) throws IOException {
        generator.writeArrayFieldStart("features");
        for (PropertySchema propertySchema : propertySchemas) {
            if (propertySchema.dataType() == DataType.Float || propertySchema.dataType() == DataType.Double) {
                writeNumericalFeatureForFloat(label, propertySchema);
            }
            if (propertySchema.dataType() == DataType.Short ||
                    propertySchema.dataType() == DataType.Integer ||
                    propertySchema.dataType() == DataType.Long) {
                writeNumericalFeatureForInt(label, propertySchema);
            }
            if (propertySchema.dataType() == DataType.String) {
                writeCategoricalFeatureForString(label, propertySchema);
            }
        }
        generator.writeEndArray();
    }

    private void writeCategoricalFeatureForString(Label label, PropertySchema propertySchema) throws IOException {

        if (config.hasWord2VecSpecificationForNodeTypeAndColumn(label, propertySchema.nameWithoutDataType())){
            writeWord2VecFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "category");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(propertySchema.nameWithoutDataType());
            generator.writeEndArray();
            if (propertySchema.isMultiValue()) {
                generator.writeStringField("separator", ";");
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
        generator.writeString(propertySchema.nameWithoutDataType());
        generator.writeEndArray();
        generator.writeArrayFieldStart("language");
        for (String language : word2VecConfig.languages()) {
            generator.writeString(language);
        }
        generator.writeEndArray();
        generator.writeStringField("node_type", label.labelsAsString());
        generator.writeEndObject();
    }

    private void writeNumericalFeatureForFloat(Label label, PropertySchema propertySchema) throws IOException {

        if (config.hasNumericalBucketSpecificationForNodeType(label, propertySchema.nameWithoutDataType())){
            writeNumericalBucketFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "numerical");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(propertySchema.nameWithoutDataType());
            generator.writeEndArray();
            if (propertySchema.isMultiValue()) {
                generator.writeStringField("separator", ";");
            }
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }


    }

    private void writeNumericalFeatureForInt(Label label, PropertySchema propertySchema) throws IOException {
        if (config.hasNumericalBucketSpecificationForNodeType(label, propertySchema.nameWithoutDataType())){
            writeNumericalBucketFeature(label, propertySchema);
        } else {
            generator.writeStartObject();
            generator.writeStringField("feat_type", "node");
            generator.writeStringField("sub_feat_type", "numerical");
            generator.writeArrayFieldStart("cols");
            generator.writeString("~id");
            generator.writeString(propertySchema.nameWithoutDataType());
            generator.writeEndArray();
            generator.writeStringField("norm", "min-max");
            generator.writeStringField("node_type", label.labelsAsString());
            generator.writeEndObject();
        }
    }

    private void writeNumericalBucketFeature(Label label, PropertySchema propertySchema) throws IOException {
        TrainingJobConfig.NumericalBucketFeatureConfig featureConfig =
                config.getNumericalBucketSpecificationForNodeType(label, propertySchema.nameWithoutDataType());

        if (propertySchema.isMultiValue()){
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
            generator.writeString(propertySchema.nameWithoutDataType());
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
            for (String outputId : outputIds) {
                generator.writeStartObject();
                writeFileName(graphElementType, outputId);
                writeSeparator();

                generator.writeArrayFieldStart("edges");
                generator.writeStartObject();
                writeEdgeSpecType();
                writeCols();
                writeEdgeType(edgeLabel);
                generator.writeEndObject();
                generator.writeEndArray();

                if (config.hasEdgeClassificationSpecificationForEdgeType(edgeLabel)) {
                    writeEdgeClassLabel(edgeLabel);
                }

                generator.writeEndObject();
            }
        }
    }

    private void writeEdgeClassLabel(Label label) throws IOException {
        generator.writeArrayFieldStart("labels");
        generator.writeStartObject();
        generator.writeStringField("label_type", "edge");
        generator.writeStringField("sub_label_type", "edge_class_label");
        generator.writeArrayFieldStart("cols");
        generator.writeString("~from");
        generator.writeString("~to");
        generator.writeEndArray();
        writeSplitRates();
        generator.writeArrayFieldStart("edge_type");
        generator.writeString(label.fromLabelsAsString());
        generator.writeString(label.labelsAsString());
        generator.writeString(label.toLabelsAsString());
        generator.writeEndArray();
        generator.writeEndObject();
        generator.writeEndArray();
    }

    private void writeSeparator() throws IOException {
        generator.writeStringField("separator", ",");
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
