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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobTrainingConfigurationFileWriterFeatureTest {

    @Test
    public void shouldWriteNewObjectForEach() throws IOException {

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);
        nodeSchemas.addLabelSchema(new LabelSchema(new Label("Person")), Arrays.asList("person-1.csv", "person-2.csv"));
        edgeSchemas.addLabelSchema(new LabelSchema(new Label("follows")), Arrays.asList("follows-1.csv", "follows-2.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(4, graph.size());

        ArrayNode array = (ArrayNode) graph;

        assertEquals("nodes/person-1.csv", array.get(0).path("file_name").textValue());
        assertEquals("nodes/person-2.csv", array.get(1).path("file_name").textValue());
        assertEquals("edges/follows-1.csv", array.get(2).path("file_name").textValue());
        assertEquals("edges/follows-2.csv", array.get(3).path("file_name").textValue());
    }

    @Test
    public void everyObjectShouldHaveACommaSeparator() throws IOException {
        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);
        nodeSchemas.addLabelSchema(new LabelSchema(new Label("Person")), Collections.singletonList("person-1.csv"));
        edgeSchemas.addLabelSchema(new LabelSchema(new Label("follows")), Collections.singletonList("follows-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(2, graph.size());

        ArrayNode array = (ArrayNode) graph;

        assertEquals(",", array.get(0).path("separator").textValue());
        assertEquals(",", array.get(1).path("separator").textValue());
    }

    @Test
    public void edgesShouldIncludeEdgeSpec() throws IOException {
        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);
        edgeSchemas.addLabelSchema(new LabelSchema(
                        new Label(
                                "follows",
                                Arrays.asList("Person", "Admin"),
                                Arrays.asList("Person", "Temp"))),
                Collections.singletonList("follows-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode edges = (ArrayNode) array.get(0).path("edges");

        assertEquals(1, edges.size());

        JsonNode edge = edges.get(0);

        assertEquals("edge", edge.path("edge_spec_type").textValue());

        ArrayNode cols = (ArrayNode) edge.path("cols");

        assertEquals("~from", cols.get(0).textValue());
        assertEquals("~to", cols.get(1).textValue());

        ArrayNode edgeType = (ArrayNode) edge.path("edge_type");

        assertEquals("Admin;Person", edgeType.get(0).textValue());
        assertEquals("follows", edgeType.get(1).textValue());
        assertEquals("Person;Temp", edgeType.get(2).textValue());
    }

    @Test
    public void singleValueFloatFeatureForVertex() throws IOException {

        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Arrays.asList("Person", "Admin")));
        labelSchema.put("rating", new PropertySchema("rating", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("numerical", feature.path("sub_feat_type").textValue());
        assertEquals("Admin;Person", feature.path("node_type").textValue());
        assertEquals("min-max", feature.path("norm").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("rating", cols.get(1).textValue());

        assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void shouldNotIncludeFeatureForMultiValueFloatFeatureForVertex() throws IOException {

        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Collections.singletonList("Movie")));
        labelSchema.put("encoding", new PropertySchema("encoding", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(0, features.size());
    }

    @Test
    public void intFeatureForVertex() throws IOException {

        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Arrays.asList("Person", "Admin")));
        labelSchema.put("age", new PropertySchema("age", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("numerical", feature.path("sub_feat_type").textValue());
        assertEquals("min-max", feature.path("norm").textValue());
        assertEquals("Admin;Person", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("age", cols.get(1).textValue());

        assertTrue(feature.path("separator").isMissingNode());

    }

    @Test
    public void singleValueStringFeatureForVertex() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Collections.singletonList("Movie")));
        labelSchema.put("class", new PropertySchema("class", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("category", feature.path("sub_feat_type").textValue());
        assertEquals("Movie", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("class", cols.get(1).textValue());

        assertTrue(feature.path("norm").isMissingNode());
        assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void multiValueStringFeatureForVertex() throws IOException {

        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema(new Label(Collections.singletonList("Movie")));
        labelSchema.put("movieType", new PropertySchema("movieType", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("category", feature.path("sub_feat_type").textValue());
        assertEquals("Movie", feature.path("node_type").textValue());
        assertEquals(";", feature.path("separator").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("movieType", cols.get(1).textValue());

        assertTrue(feature.path("norm").isMissingNode());
    }

    @Test
    public void shouldAddWord2VecFeatureIfSpecifiedInConfig() throws IOException {
        DataType dataType = DataType.String;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        Label movieLabel = new Label(Collections.singletonList("Movie"));
        LabelSchema labelSchema = new LabelSchema(movieLabel);
        labelSchema.put("genre", new PropertySchema("genre", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                TrainingJobConfigBuilder.builder()
                        .withWord2VecNodeFeature(
                                movieLabel,
                                "genre",
                                "en_core_web_lg", "fr_core_news_lg")
                        .build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("word2vec", feature.path("sub_feat_type").textValue());
        assertEquals("Movie", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("genre", cols.get(1).textValue());

        ArrayNode language = (ArrayNode) feature.path("language");

        assertEquals(2, language.size());

        assertEquals("en_core_web_lg", language.get(0).textValue());
        assertEquals("fr_core_news_lg", language.get(1).textValue());

        assertTrue(feature.path("norm").isMissingNode());
        assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void shouldNumericalBucketFeatureIfSpecifiedInConfig() throws IOException {
        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        Label movieLabel = new Label(Collections.singletonList("Movie"));
        LabelSchema labelSchema = new LabelSchema(movieLabel);
        labelSchema.put("score", new PropertySchema("score", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                TrainingJobConfigBuilder.builder()
                .withNumericalBucketFeature(movieLabel, "score", new TrainingJobWriterConfig.Range(1, 100), 10, 2)
                .build())
                .write();

        JsonNode graph = output.graph();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("bucket_numerical", feature.path("sub_feat_type").textValue());
        assertEquals("Movie", feature.path("node_type").textValue());
        assertEquals(10, feature.path("bucket_cnt").intValue());
        assertEquals(2, feature.path("slide_window_size").intValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("score", cols.get(1).textValue());

        ArrayNode range = (ArrayNode) feature.path("range");

        assertEquals(2, range.size());

        assertEquals(1, range.get(0).intValue());
        assertEquals(100, range.get(1).intValue());

        assertTrue(feature.path("norm").isMissingNode());
        assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void shouldAddNumericalBucketFeatureForAllNumberTypes() throws IOException {
        Collection<DataType> dataTypes = Arrays.asList(DataType.Byte, DataType.Integer, DataType.Double, DataType.Float, DataType.Long, DataType.Short);

        boolean isNullable = false;
        boolean isMultiValue = false;

        for (DataType dataType : dataTypes) {
            GraphSchema graphSchema = new GraphSchema();
            GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

            Label movieLabel = new Label(Collections.singletonList("Movie"));
            LabelSchema labelSchema = new LabelSchema(movieLabel);
            labelSchema.put("score", new PropertySchema("score", isNullable, dataType, isMultiValue));

            nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

            Output output = new Output();

            new JobTrainingConfigurationFileWriter(
                    graphSchema,
                    output.generator(),
                    JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE,
                    PrinterOptions.NULL_OPTIONS,
                    TrainingJobConfigBuilder.builder()
                    .withNumericalBucketFeature(movieLabel, "score", new TrainingJobWriterConfig.Range(1, 100), 10, 2)
                    .build())
                    .write();

            JsonNode graph = output.graph();

            assertEquals(1, graph.size());

            ArrayNode array = (ArrayNode) graph;
            ArrayNode features = (ArrayNode) array.get(0).path("features");

            assertEquals(1, features.size());

            JsonNode feature = features.get(0);

            assertEquals("node", feature.path("feat_type").textValue());
            assertEquals("bucket_numerical", feature.path("sub_feat_type").textValue());
        }
    }

    @Test
    public void shouldAddWarningIfAttemptingToCreateNumericalBucketFeatureForMultiValueDataType() throws IOException {
        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        Label movieLabel = new Label(Collections.singletonList("Movie"));
        LabelSchema labelSchema = new LabelSchema(movieLabel);
        labelSchema.put("score", new PropertySchema("score", isNullable, dataType, isMultiValue));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(
                graphSchema,
                output.generator(),
                JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS,
                TrainingJobConfigBuilder.builder()
                .withNumericalBucketFeature(movieLabel, "score", new TrainingJobWriterConfig.Range(1, 100), 10, 2)
                .build())
                .write();

        JsonNode graph = output.graph();

        ArrayNode warnings = output.warnings();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;

        assertTrue(array.get(0).path("labels").isMissingNode());

        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(0, features.size());

        assertEquals(1, warnings.size());
        assertEquals("Unable to add numerical bucket feature: Property 'score' of node type 'Movie' is a multi-value property.", warnings.get(0).textValue());
    }

    @Test
    public void singleValueNumericFeatureForEdge() throws IOException {

        Collection<DataType> dataTypes = Arrays.asList(DataType.Byte, DataType.Integer, DataType.Double, DataType.Float, DataType.Long, DataType.Short);

        boolean isNullable = false;
        boolean isMultiValue = false;

        for (DataType dataType : dataTypes) {
            GraphSchema graphSchema = new GraphSchema();
            GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);

            LabelSchema labelSchema = new LabelSchema(new Label("knows", Collections.singletonList("Person"), Collections.singletonList("Person")));
            labelSchema.put("strength", new PropertySchema("strength", isNullable, dataType, isMultiValue));

            edgeSchemas.addLabelSchema(labelSchema, Collections.singletonList("knows-1.csv"));

            Output output = new Output();

            new JobTrainingConfigurationFileWriter(graphSchema, output.generator(), JobTrainingConfigurationFileWriter.COLUMN_NAME_WITHOUT_DATATYPE, PrinterOptions.NULL_OPTIONS).write();

            JsonNode graph = output.graph();

            assertEquals(1, graph.size());

            ArrayNode array = (ArrayNode) graph;
            ArrayNode features = (ArrayNode) array.get(0).path("features");

            assertEquals(1, features.size());

            JsonNode feature = features.get(0);

            assertEquals("edge", feature.path("feat_type").textValue());
            assertEquals("numerical", feature.path("sub_feat_type").textValue());
            assertEquals("min-max", feature.path("norm").textValue());

            ArrayNode cols = (ArrayNode) feature.path("cols");

            assertEquals(3, cols.size());

            assertEquals("~from", cols.get(0).textValue());
            assertEquals("~to", cols.get(1).textValue());
            assertEquals("strength", cols.get(2).textValue());

            ArrayNode edgeType = (ArrayNode) feature.path("edge_type");

            assertEquals(3, edgeType.size());

            assertEquals("Person", edgeType.get(0).textValue());
            assertEquals("knows", edgeType.get(1).textValue());
            assertEquals("Person", edgeType.get(2).textValue());

            assertTrue(feature.path("separator").isMissingNode());

        }
    }

}