package com.amazonaws.services.neptune.io;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.PrintWriter;
import java.util.Map;

public class EdgeWriter implements GraphElementHandler<Path> {

    private final PrintWriter writer;
    private final PropertyWriter propertyWriter;

    public EdgeWriter(PrintWriter writer, PropertyWriter propertyWriter) {
        this.writer = writer;
        this.propertyWriter = propertyWriter;
    }

    @Override
    public void handle(Path path, boolean allowStructuralElements) {
        String from = path.get(3);
        String to = path.get(1);
        Map<?, Object> properties = path.get(0);
        String id = String.valueOf(properties.get(T.id));
        String label = String.valueOf(properties.get(T.label));
        writer.printf("%s,%s,%s,%s", id, label, from, to);
        propertyWriter.handle(properties, writer);
        writer.print(System.lineSeparator());
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
