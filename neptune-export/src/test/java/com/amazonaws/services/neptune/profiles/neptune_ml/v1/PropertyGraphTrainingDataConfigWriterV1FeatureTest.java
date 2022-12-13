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
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyGraphTrainingDataConfigWriterV1FeatureTest {

    @Test
    public void shouldWriteNewObjectForEach() throws IOException {

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.edges);
        nodeSchemas.addLabelSchema(new LabelSchema(new Label("Person")), Arrays.asList("person-1.csv", "person-2.csv"));
        edgeSchemas.addLabelSchema(new LabelSchema(new Label("follows")), Arrays.asList("follows-1.csv", "follows-2.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(4, graph.size());

        ArrayNode array = (ArrayNode) graph;

        Assert.assertEquals("nodes/person-1.csv", array.get(0).path("file_name").textValue());
        Assert.assertEquals("nodes/person-2.csv", array.get(1).path("file_name").textValue());
        Assert.assertEquals("edges/follows-1.csv", array.get(2).path("file_name").textValue());
        Assert.assertEquals("edges/follows-2.csv", array.get(3).path("file_name").textValue());
    }

    @Test
    public void everyObjectShouldHaveACommaSeparator() throws IOException {
        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.edges);
        nodeSchemas.addLabelSchema(new LabelSchema(new Label("Person")), Collections.singletonList("person-1.csv"));
        edgeSchemas.addLabelSchema(new LabelSchema(new Label("follows")), Collections.singletonList("follows-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(2, graph.size());

        ArrayNode array = (ArrayNode) graph;

        Assert.assertEquals(",", array.get(0).path("separator").textValue());
        Assert.assertEquals(",", array.get(1).path("separator").textValue());
    }

    @Test
    public void edgesShouldIncludeEdgeSpec() throws IOException {
        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.edges);
        edgeSchemas.addLabelSchema(new LabelSchema(
                        new Label(
                                "follows",
                                Arrays.asList("Person", "Admin"),
                                Arrays.asList("Person", "Temp"))),
                Collections.singletonList("follows-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode edges = (ArrayNode) array.get(0).path("edges");

        Assert.assertEquals(1, edges.size());

        JsonNode edge = edges.get(0);

        Assert.assertEquals("edge", edge.path("edge_spec_type").textValue());

        ArrayNode cols = (ArrayNode) edge.path("cols");

        Assert.assertEquals("~from", cols.get(0).textValue());
        Assert.assertEquals("~to", cols.get(1).textValue());

        ArrayNode edgeType = (ArrayNode) edge.path("edge_type");

        Assert.assertEquals("Admin;Person", edgeType.get(0).textValue());
        Assert.assertEquals("follows", edgeType.get(1).textValue());
        Assert.assertEquals("Person;Temp", edgeType.get(2).textValue());
    }

    @Test
    public void singleValueFloatFeatureForVertex() throws IOException {

        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Arrays.asList("Person", "Admin")));
        labelSchema.put("rating", new PropertySchema("rating", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("numerical", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Admin;Person", feature.path("node_type").textValue());
        Assert.assertEquals("min-max", feature.path("norm").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("rating", cols.get(1).textValue());

        Assert.assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void shouldNotIncludeFeatureForMultiValueFloatFeatureForVertex() throws IOException {

        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Collections.singletonList("Movie")));
        labelSchema.put("encoding", new PropertySchema("encoding", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(0, features.size());
    }

    @Test
    public void intFeatureForVertex() throws IOException {

        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Arrays.asList("Person", "Admin")));
        labelSchema.put("age", new PropertySchema("age", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("numerical", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("min-max", feature.path("norm").textValue());
        Assert.assertEquals("Admin;Person", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("age", cols.get(1).textValue());

        Assert.assertTrue(feature.path("separator").isMissingNode());

    }

    @Test
    public void singleValueStringFeatureForVertex() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Collections.singletonList("Movie")));
        labelSchema.put("class", new PropertySchema("class", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("category", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Movie", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("class", cols.get(1).textValue());

        Assert.assertTrue(feature.path("norm").isMissingNode());
        Assert.assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void multiValueStringFeatureForVertex() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Collections.singletonList("Movie")));
        labelSchema.put("movieType", new PropertySchema("movieType", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("category", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Movie", feature.path("node_type").textValue());
        Assert.assertEquals(";", feature.path("separator").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("movieType", cols.get(1).textValue());

        Assert.assertTrue(feature.path("norm").isMissingNode());
    }

    @Test
    public void shouldAddWord2VecFeatureIfSpecifiedInConfig() throws IOException {
        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        Label movieLabel = new Label(Collections.singletonList("Movie"));
        LabelSchema labelSchema = new LabelSchema(movieLabel);
        labelSchema.put("genre", new PropertySchema("genre", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                PropertyGraphTrainingDataConfigBuilderV1.builder()
                        .withWord2VecNodeFeature(
                                movieLabel,
                                "genre",
                                "en_core_web_lg", "fr_core_news_lg")
                        .build())
                .write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("word2vec", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Movie", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("genre", cols.get(1).textValue());

        ArrayNode language = (ArrayNode) feature.path("language");

        Assert.assertEquals(2, language.size());

        Assert.assertEquals("en_core_web_lg", language.get(0).textValue());
        Assert.assertEquals("fr_core_news_lg", language.get(1).textValue());

        Assert.assertTrue(feature.path("norm").isMissingNode());
        Assert.assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void shouldNumericalBucketFeatureIfSpecifiedInConfig() throws IOException {
        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        Label movieLabel = new Label(Collections.singletonList("Movie"));
        LabelSchema labelSchema = new LabelSchema(movieLabel);
        labelSchema.put("score", new PropertySchema("score", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                PropertyGraphTrainingDataConfigBuilderV1.builder()
                .withNumericalBucketFeature(movieLabel, "score", new Range(1, 100), 10, 2)
                .build())
                .write();

        JsonNode graph = output.graph();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        Assert.assertEquals("node", feature.path("feat_type").textValue());
        Assert.assertEquals("bucket_numerical", feature.path("sub_feat_type").textValue());
        Assert.assertEquals("Movie", feature.path("node_type").textValue());
        Assert.assertEquals(10, feature.path("bucket_cnt").intValue());
        Assert.assertEquals(2, feature.path("slide_window_size").intValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        Assert.assertEquals(2, cols.size());

        Assert.assertEquals("~id", cols.get(0).textValue());
        Assert.assertEquals("score", cols.get(1).textValue());

        ArrayNode range = (ArrayNode) feature.path("range");

        Assert.assertEquals(2, range.size());

        Assert.assertEquals(1, range.get(0).intValue());
        Assert.assertEquals(100, range.get(1).intValue());

        Assert.assertTrue(feature.path("norm").isMissingNode());
        Assert.assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void shouldAddNumericalBucketFeatureForAllNumberTypes() throws IOException {
        Collection<DataType> dataTypes = Arrays.asList(DataType.Byte, DataType.Integer, DataType.Double, DataType.Float, DataType.Long, DataType.Short);

        boolean isNullable = false;
        boolean isMultiValue = false;

        for (DataType dataType : dataTypes) {
            GraphSchema graphSchema = new GraphSchema();
            GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

            Label movieLabel = new Label(Collections.singletonList("Movie"));
            LabelSchema labelSchema = new LabelSchema(movieLabel);
            labelSchema.put("score", new PropertySchema("score", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

            nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

            Output output = new Output();

            new PropertyGraphTrainingDataConfigWriterV1(
                    graphSchema,
                    output.generator(),
                    PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE,
                    PrinterOptions.NULL_OPTIONS,
                    PropertyGraphTrainingDataConfigBuilderV1.builder()
                    .withNumericalBucketFeature(movieLabel, "score", new Range(1, 100), 10, 2)
                    .build())
                    .write();

            JsonNode graph = output.graph();

            Assert.assertEquals(1, graph.size());

            ArrayNode array = (ArrayNode) graph;
            ArrayNode features = (ArrayNode) array.get(0).path("features");

            Assert.assertEquals(1, features.size());

            JsonNode feature = features.get(0);

            Assert.assertEquals("node", feature.path("feat_type").textValue());
            Assert.assertEquals("bucket_numerical", feature.path("sub_feat_type").textValue());
        }
    }

    @Test
    public void shouldAddWarningIfAttemptingToCreateNumericalBucketFeatureForMultiValueDataType() throws IOException {
        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.nodes);

        Label movieLabel = new Label(Collections.singletonList("Movie"));
        LabelSchema labelSchema = new LabelSchema(movieLabel);
        labelSchema.put("score", new PropertySchema("score", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV1(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS,
                PropertyGraphTrainingDataConfigBuilderV1.builder()
                .withNumericalBucketFeature(movieLabel, "score", new Range(1, 100), 10, 2)
                .build())
                .write();

        JsonNode graph = output.graph();

        ArrayNode warnings = output.warnings();

        Assert.assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;

        Assert.assertTrue(array.get(0).path("labels").isMissingNode());

        ArrayNode features = (ArrayNode) array.get(0).path("features");

        Assert.assertEquals(0, features.size());

        Assert.assertEquals(1, warnings.size());
        Assert.assertEquals("Unable to add numerical bucket feature: Property 'score' of node type 'Movie' is a multi-value property.", warnings.get(0).textValue());
    }

    @Test
    public void singleValueNumericFeatureForEdge() throws IOException {

        Collection<DataType> dataTypes = Arrays.asList(DataType.Byte, DataType.Integer, DataType.Double, DataType.Float, DataType.Long, DataType.Short);

        boolean isNullable = false;
        boolean isMultiValue = false;

        for (DataType dataType : dataTypes) {
            GraphSchema graphSchema = new GraphSchema();
            GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementType.edges);

            LabelSchema labelSchema = new LabelSchema(new Label("knows", Collections.singletonList("Person"), Collections.singletonList("Person")));
            labelSchema.put("strength", new PropertySchema("strength", isNullable, dataType, isMultiValue, EnumSet.noneOf(DataType.class)));

            edgeSchemas.addLabelSchema(labelSchema, Collections.singletonList("knows-1.csv"));

            Output output = new Output();

            new PropertyGraphTrainingDataConfigWriterV1(graphSchema, output.generator(), PropertyGraphTrainingDataConfigWriterV1.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

            JsonNode graph = output.graph();

            Assert.assertEquals(1, graph.size());

            ArrayNode array = (ArrayNode) graph;
            ArrayNode features = (ArrayNode) array.get(0).path("features");

            Assert.assertEquals(1, features.size());

            JsonNode feature = features.get(0);

            Assert.assertEquals("edge", feature.path("feat_type").textValue());
            Assert.assertEquals("numerical", feature.path("sub_feat_type").textValue());
            Assert.assertEquals("min-max", feature.path("norm").textValue());

            ArrayNode cols = (ArrayNode) feature.path("cols");

            Assert.assertEquals(3, cols.size());

            Assert.assertEquals("~from", cols.get(0).textValue());
            Assert.assertEquals("~to", cols.get(1).textValue());
            Assert.assertEquals("strength", cols.get(2).textValue());

            ArrayNode edgeType = (ArrayNode) feature.path("edge_type");

            Assert.assertEquals(3, edgeType.size());

            Assert.assertEquals("Person", edgeType.get(0).textValue());
            Assert.assertEquals("knows", edgeType.get(1).textValue());
            Assert.assertEquals("Person", edgeType.get(2).textValue());

            Assert.assertTrue(feature.path("separator").isMissingNode());

        }
    }

}