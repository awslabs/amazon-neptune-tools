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

import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobTrainingConfigurationFileWriterLabelTest {

    @Test
    public void shouldAddNodeClassLabelIfSpecifiedInConfig() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;
        Label personLabel = new Label(Collections.singletonList("Person"));

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(personLabel);
        labelSchema.put("role", new PropertySchema("role", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NO_HEADERS,
                TrainingJobConfigBuilder.builder()
                        .withNodeClassLabel(personLabel, "role")
                        .build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode labels = (ArrayNode) array.get(0).path("labels");

        assertEquals(1, labels.size());

        JsonNode label = labels.get(0);

        assertEquals("node", label.path("label_type").textValue());
        assertEquals("node_class_label", label.path("sub_label_type").textValue());

        ArrayNode cols = (ArrayNode) label.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("role", cols.get(1).textValue());

        ArrayNode splitRates = (ArrayNode) label.path("split_rate");

        assertEquals(3, splitRates.size());

        assertEquals(0.7, splitRates.get(0).doubleValue(), 0.0);
        assertEquals(0.1, splitRates.get(1).doubleValue(), 0.0);
        assertEquals(0.2, splitRates.get(2).doubleValue(), 0.0);

        assertEquals("Person", label.path("node_type").textValue());

        assertTrue(label.path("separator").isMissingNode());
    }

    @Test
    public void shouldAddWarningIfColumnDoesNotExistForNodeClassLabel() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;
        Label personLabel = new Label(Collections.singletonList("Person"));

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(personLabel);
        labelSchema.put("role", new PropertySchema("role", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NO_HEADERS,
                TrainingJobConfigBuilder.builder()
                        .withNodeClassLabel(personLabel, "does-not-exist")
                        .build())
                .write();

        JsonNode graph = output.graph();
        ArrayNode warnings = output.warnings();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;

        assertTrue(array.get(0).path("labels").isMissingNode());

        assertEquals(1, warnings.size());
        assertEquals("Unable to add node class label: Node of type 'Person' does not contain property 'does-not-exist'.", warnings.get(0).textValue());
    }

    @Test
    public void shouldAddSeparatorIfNodeClassLabelIsMultiValued() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = true;
        Label personLabel = new Label(Collections.singletonList("Person"));

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(personLabel);
        labelSchema.put("role", new PropertySchema("role", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NO_HEADERS,
                TrainingJobConfigBuilder.builder()
                        .withNodeClassLabel(personLabel, "role")
                        .build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode labels = (ArrayNode) array.get(0).path("labels");

        assertEquals(1, labels.size());

        JsonNode label = labels.get(0);

        assertEquals(";", label.path("separator").textValue());
    }

    @Test
    public void shouldAddEdgeClassLabelIfSpecifiedInConfig() throws IOException {

        Label knowsLabel = new Label("knows",
                Collections.singletonList("Person"),
                Collections.singletonList("Person"));

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);

        LabelSchema labelSchema = new LabelSchema(knowsLabel);
        labelSchema.put("contact", new PropertySchema("contact", isNullable, dataType, isMultiValue));

        edgeSchemas.addLabelSchema(labelSchema, Collections.singletonList("knows-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NO_HEADERS,
                TrainingJobConfigBuilder.builder()
                        .withEdgeClassLabel(knowsLabel, "contact")
                        .build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode labels = (ArrayNode) array.get(0).path("labels");

        assertEquals(1, labels.size());

        JsonNode label = labels.get(0);

        assertEquals("edge", label.path("label_type").textValue());
        assertEquals("edge_class_label", label.path("sub_label_type").textValue());

        ArrayNode cols = (ArrayNode) label.path("cols");

        assertEquals(3, cols.size());

        assertEquals("~from", cols.get(0).textValue());
        assertEquals("~to", cols.get(1).textValue());
        assertEquals("contact", cols.get(2).textValue());

        ArrayNode splitRates = (ArrayNode) label.path("split_rate");

        assertEquals(3, splitRates.size());

        assertEquals(0.7, splitRates.get(0).doubleValue(), 0.0);
        assertEquals(0.1, splitRates.get(1).doubleValue(), 0.0);
        assertEquals(0.2, splitRates.get(2).doubleValue(), 0.0);

        ArrayNode edgeType = (ArrayNode) label.path("edge_type");

        assertEquals("Person", edgeType.get(0).textValue());
        assertEquals("knows", edgeType.get(1).textValue());
        assertEquals("Person", edgeType.get(2).textValue());

        assertTrue(label.path("separator").isMissingNode());
    }

    @Test
    public void shouldAddWarningIfColumnDoesNotExistForEdgeClassLabel() throws IOException {

        Label knowsLabel = new Label("knows",
                Collections.singletonList("Person"),
                Collections.singletonList("Person"));

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);

        LabelSchema labelSchema = new LabelSchema(knowsLabel);
        labelSchema.put("contact", new PropertySchema("contact", isNullable, dataType, isMultiValue));

        edgeSchemas.addLabelSchema(labelSchema, Collections.singletonList("knows-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NO_HEADERS,
                TrainingJobConfigBuilder.builder()
                .withEdgeClassLabel(knowsLabel, "does-not-exist")
                .build())
                .write();

        JsonNode graph = output.graph();
        ArrayNode warnings = output.warnings();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;

        assertTrue(array.get(0).path("labels").isMissingNode());

        assertEquals(1, warnings.size());
        assertEquals("Unable to add edge class label: Edge of type 'knows' does not contain property 'does-not-exist'.", warnings.get(0).textValue());
    }

    @Test
    public void shouldAddSeparatorIfEdgeClassLabelIsMultiValued() throws IOException {

        Label knowsLabel = new Label("knows",
                Collections.singletonList("Person"),
                Collections.singletonList("Person"));

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);

        LabelSchema labelSchema = new LabelSchema(knowsLabel);
        labelSchema.put("contact", new PropertySchema("contact", isNullable, dataType, isMultiValue));

        edgeSchemas.addLabelSchema(labelSchema, Collections.singletonList("knows-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NO_HEADERS,
                TrainingJobConfigBuilder.builder()
                .withEdgeClassLabel(knowsLabel, "contact")
                .build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode labels = (ArrayNode) array.get(0).path("labels");

        assertEquals(1, labels.size());

        JsonNode label = labels.get(0);

        assertEquals(";", label.path("separator").textValue());
    }

}
