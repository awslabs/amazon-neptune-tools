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
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementType;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementTypes;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class JobTrainingConfigurationFileWriter {

    private final GraphSchema graphSchema;
    private final JsonGenerator generator;

    public JobTrainingConfigurationFileWriter(GraphSchema graphSchema, JsonGenerator generator) {
        this.graphSchema = graphSchema;
        this.generator = generator;
    }

    public void write() throws IOException {

        generator.writeStartObject();
        generator.writeArrayFieldStart("graph");

        writeNodes();
        writeEdges();

        generator.writeEndArray();
        generator.flush();
    }

    private void writeNodes() throws IOException {
        GraphElementType<Map<String, Object>> graphElementType = GraphElementTypes.Nodes;
        GraphElementSchemas graphElementSchemas = graphSchema.graphElementSchemasFor(graphElementType);

        for (Label nodeLabel : graphElementSchemas.labels()) {
            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(nodeLabel);
            for (String outputId : outputIds) {
                generator.writeStartObject();
                generator.writeStringField("file_name", String.format("%s/%s", graphElementType.name(), new File(outputId).getName()));
                generator.writeStringField("separator", ",");
                generator.writeEndObject();
            }
        }
    }

    private void writeEdges() throws IOException {
        GraphElementType<Map<String, Object>> graphElementType = GraphElementTypes.Edges;
        GraphElementSchemas graphElementSchemas = graphSchema.graphElementSchemasFor(graphElementType);

        for (Label nodeLabel : graphElementSchemas.labels()) {
            Collection<String> outputIds = graphElementSchemas.getOutputIdsFor(nodeLabel);
            for (String outputId : outputIds) {
                generator.writeStartObject();
                generator.writeStringField("file_name", String.format("%s/%s", graphElementType.name(), new File(outputId).getName()));
                generator.writeStringField("separator", ",");
                generator.writeEndObject();
            }
        }
    }


}
