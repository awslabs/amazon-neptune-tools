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
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobTrainingConfigurationFileWriteFeatureOverrideTests {

    @Test
    public void shouldOverrideNumericalWithCategoricalFeature() throws IOException {
        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        Label label = new Label(Arrays.asList("Person", "Admin"));
        LabelSchema labelSchema = new LabelSchema(label);
        labelSchema.put("rating", new PropertySchema("rating", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                TrainingJobConfigBuilder.builder()
                        .withNodeFeatureOverride(
                                new TrainingJobWriterConfig.FeatureOverrideConfig(
                                        label,
                                        Collections.singletonList("rating"),
                                        FeatureType.category,
                                        null,
                                        ",")).build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("category", feature.path("sub_feat_type").textValue());
        assertEquals("Admin;Person", feature.path("node_type").textValue());
        assertEquals(",", feature.path("separator").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("rating", cols.get(1).textValue());

        assertTrue(feature.path("norm").isMissingNode());
    }

    @Test
    public void shouldCreateMultiCategoricalFeature() throws IOException {

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        Label label = new Label(Arrays.asList("Person", "Admin"));
        LabelSchema labelSchema = new LabelSchema(label);
        labelSchema.put("rating", new PropertySchema("rating", false, DataType.Float, false));
        labelSchema.put("job", new PropertySchema("job", false, DataType.String, false));
        labelSchema.put("rank", new PropertySchema("rank", false, DataType.Integer, false));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                TrainingJobConfigBuilder.builder()
                        .withNodeFeatureOverride(
                                new TrainingJobWriterConfig.FeatureOverrideConfig(
                                        label,
                                        Arrays.asList("job","rank"),
                                        FeatureType.category,
                                        null,
                                        ",")).build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(2, features.size());

        JsonNode feature = features.get(1);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("category", feature.path("sub_feat_type").textValue());
        assertEquals("Admin;Person", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(3, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("job", cols.get(1).textValue());
        assertEquals("rank", cols.get(2).textValue());

        assertTrue(feature.path("norm").isMissingNode());
        assertTrue(feature.path("separator").isMissingNode());
    }
}
