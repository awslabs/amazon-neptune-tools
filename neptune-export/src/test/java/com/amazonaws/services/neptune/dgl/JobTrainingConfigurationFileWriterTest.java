package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementTypes;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;

import static org.junit.Assert.*;

public class JobTrainingConfigurationFileWriterTest {

    @Test
    public void shouldWriteArrayOfFileNamesForEachElement() throws IOException {

        GraphSchema graphSchema = new GraphSchema();
        GraphElementSchemas nodes = graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes);
        GraphElementSchemas edges = graphSchema.graphElementSchemasFor(GraphElementTypes.Edges);
        nodes.addLabelSchema(new LabelSchema(new Label("Person")), Arrays.asList("person-1.csv", "person-2.csv"));
        edges.addLabelSchema(new LabelSchema(new Label("follows")), Arrays.asList("follows-1.csv", "follows-2.csv"));

        StringWriter writer = new StringWriter();

        new JobTrainingConfigurationFileWriter(graphSchema, createJsonGenerator(writer)).write();

        System.out.println(writer.toString() );
    }

    private JsonGenerator createJsonGenerator(Writer writer) throws IOException {
        JsonGenerator generator = new JsonFactory().createGenerator(writer);
        generator.setPrettyPrinter(new DefaultPrettyPrinter());
        return generator;
    }

}