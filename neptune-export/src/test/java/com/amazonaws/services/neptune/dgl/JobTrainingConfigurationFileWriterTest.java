package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobTrainingConfigurationFileWriterTest {

    @Test
    public void shouldWriteNewObjectForEach() throws IOException {

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);
        GraphElementSchemas edgeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);
        nodeSchemas.addLabelSchema(new LabelSchema(new Label("Person")), Arrays.asList("person-1.csv", "person-2.csv"));
        edgeSchemas.addLabelSchema(new LabelSchema(new Label("follows")), Arrays.asList("follows-1.csv", "follows-2.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

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

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

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
                        Collections.singletonList("follows"),
                        Arrays.asList("Person", "Admin"),
                        Arrays.asList("Person", "Temp"))),
                Collections.singletonList("follows-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode edges = (ArrayNode) array.get(0).path("edges");

        assertEquals(1, edges.size());

        JsonNode edge = edges.get(0);

        assertEquals("rel_edge", edge.path("edge_spec_type").textValue());

        ArrayNode cols = (ArrayNode) edge.path("cols");

        assertEquals("~from", cols.get(0).textValue());
        assertEquals("~to", cols.get(1).textValue());
        assertEquals("~label", cols.get(2).textValue());

        assertEquals("Admin;Person", edge.path("src_node_type").textValue());
        assertEquals("Person;Temp", edge.path("dst_node_type").textValue());
    }

    @Test
    public void singleValueFloatFeatureForVertex() throws IOException {

        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema( new Label(Arrays.asList("Person", "Admin")));
        labelSchema.put("rating", new PropertySchema("rating", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("numerical", feature.path("sub_feat_type").textValue());
        assertEquals("Admin;Person", feature.path("node_type").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("rating", cols.get(1).textValue());

        assertTrue(feature.path("norm").isMissingNode());
        assertTrue(feature.path("separator").isMissingNode());
    }

    @Test
    public void multiValueFloatFeatureForVertex() throws IOException {

        DataType dataType = DataType.Float;
        boolean isNullable = false;
        boolean isMultiValue = true;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema( new Label(Collections.singletonList("Movie")));
        labelSchema.put("encoding", new PropertySchema("encoding", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

        assertEquals(1, graph.size());

        ArrayNode array = (ArrayNode) graph;
        ArrayNode features = (ArrayNode) array.get(0).path("features");

        assertEquals(1, features.size());

        JsonNode feature = features.get(0);

        assertEquals("node", feature.path("feat_type").textValue());
        assertEquals("numerical", feature.path("sub_feat_type").textValue());
        assertEquals("Movie", feature.path("node_type").textValue());
        assertEquals(";", feature.path("separator").textValue());

        ArrayNode cols = (ArrayNode) feature.path("cols");

        assertEquals(2, cols.size());

        assertEquals("~id", cols.get(0).textValue());
        assertEquals("encoding", cols.get(1).textValue());

        assertTrue(feature.path("norm").isMissingNode());
    }

    @Test
    public void intFeatureForVertex() throws IOException {

        DataType dataType = DataType.Integer;
        boolean isNullable = false;
        boolean isMultiValue = false;

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodeSchemas = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);

        LabelSchema labelSchema = new LabelSchema( new Label(Arrays.asList("Person", "Admin")));
        labelSchema.put("age", new PropertySchema("age", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("person-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

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

        LabelSchema labelSchema = new LabelSchema( new Label(Collections.singletonList("Movie")));
        labelSchema.put("class", new PropertySchema("class", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

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

        LabelSchema labelSchema = new LabelSchema( new Label(Collections.singletonList("Movie")));
        labelSchema.put("movieType", new PropertySchema("movieType", isNullable, dataType, isMultiValue, 0, 0));

        nodeSchemas.addLabelSchema(labelSchema, Collections.singletonList("movie-1.csv"));

        Output output = new Output();

        new JobTrainingConfigurationFileWriter(graphSchema, output.generator()).write();

        JsonNode graph = output.json();

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

    private static class Output {
        private final StringWriter writer = new StringWriter();
        private final JsonGenerator generator;

        public Output() throws IOException {
            this.generator = createJsonGenerator(writer);
        }

        public JsonGenerator generator(){
            return generator;
        }

        public JsonNode json() throws JsonProcessingException {
            return new ObjectMapper().readTree(writer.toString()).path("graph");
        }

        private JsonGenerator createJsonGenerator(Writer writer) throws IOException {
            JsonGenerator generator = new JsonFactory().createGenerator(writer);
            generator.setPrettyPrinter(new DefaultPrettyPrinter());
            return generator;
        }
    }

}