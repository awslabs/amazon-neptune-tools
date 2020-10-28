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

        Map<Label, String> nodeClassLabels = new HashMap<>();
        nodeClassLabels.put(personLabel, "role");

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(personLabel);
        labelSchema.put("role", new PropertySchema("role", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                TrainingClassConfigBuilder.builder()
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
    public void shouldAddSeparatorIfNodeClassLabelIsMutiValued() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = true;
        Label personLabel = new Label(Collections.singletonList("Person"));

        Map<Label, String> nodeClassLabels = new HashMap<>();
        nodeClassLabels.put(personLabel, "role");

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(personLabel);
        labelSchema.put("role", new PropertySchema("role", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                TrainingClassConfigBuilder.builder()
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

    public static class TrainingClassConfigBuilder {

        public static TrainingClassConfigBuilder builder() {
            return new TrainingClassConfigBuilder();
        }

        Map<Label, String> nodeClassLabels = new HashMap<>();
        Collection<Double> splitRates = Arrays.asList(0.7, 0.1, 0.2);

        public TrainingClassConfigBuilder withNodeClassLabel(Label label, String column) {
            nodeClassLabels.clear();
            nodeClassLabels.put(label, column);
            return this;
        }

        public TrainingClassConfigBuilder withSplitRates(double train, double valid, double test) {
            splitRates = Arrays.asList(train, valid, test);
            return this;
        }

        public TrainingJobConfig build() {
            return new TrainingJobConfig(nodeClassLabels, splitRates);
        }
    }
}
