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

import com.amazonaws.services.neptune.profiles.neptune_ml.Output;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.FeatureOverrideConfigV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.FeatureTypeV1;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

public class PropertyGraphTrainingDataConfigWriterV1FeatureOverrideTests {

    @Test
    public void shouldOverrideNumericalWithCategoricalFeature() throws IOException {
        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        Label label = new Label(Arrays.asList("Person", "Admin"));
        LabelSchema labelSchema = new LabelSchema(label);
        labelSchema.put("rating", new PropertySchema("rating", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                PropertyGraphTrainingDataConfigBuilderV1.builder()
                        .withNodeFeatureOverride(
                                new FeatureOverrideConfigV1(
                                        label,
                                        Collections.singletonList("rating"),
                                        FeatureTypeV1.category,
                                        null,
                                        new Separator(","))).build())
                .write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("category", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Admin;Person", feature.path("node_type").textValue());
        Assert.assertEquals(",", feature.path("separator").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("rating", cols.get(1).textValue());

        Assert.assertTrue(feature.path("norm").isMissingNode());
    }

    @Test
    public void shouldCreateMultiCategoricalFeature() throws IOException {

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        Label label = new Label(Arrays.asList("Person", "Admin"));
        LabelSchema labelSchema = new LabelSchema(label);
        labelSchema.put("rating", new PropertySchema("rating", false, DataType.Float, false, EnumSet.noneOf(DataType.class)));
        labelSchema.put("job", new PropertySchema("job", false, DataType.String, false, EnumSet.noneOf(DataType.class)));
        labelSchema.put("rank", new PropertySchema("rank", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                PropertyGraphTrainingDataConfigBuilderV1.builder()
                        .withNodeFeatureOverride(
                                new FeatureOverrideConfigV1(
                                        label,
                                        Arrays.asList("job", "rank"),
                                        FeatureTypeV1.category,
                                        null,
                                        new Separator(","))).build())
                .write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(2, features.size());

        JsonNode feature = features.get(1);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("category", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Admin;Person", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(3, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("job", cols.get(1).textValue());
        Assert.assertEquals("rank", cols.get(2).textValue());

        Assert.assertTrue(feature.path("norm").isMissingNode());
        Assert.assertTrue(feature.path("separator").isMissingNode());
    }
}
