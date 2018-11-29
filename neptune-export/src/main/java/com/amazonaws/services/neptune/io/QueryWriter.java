package com.amazonaws.services.neptune.io;

import java.io.PrintWriter;
import java.util.Map;

public class QueryWriter implements GraphElementHandler<Map<?, ?>> {

    private final PrintWriter writer;
    private final PropertyWriter propertyWriter;

    public QueryWriter(PrintWriter writer, PropertyWriter propertyWriter) {
        this.writer = writer;
        this.propertyWriter = propertyWriter;
    }

    @Override
    public void handle(Map<?, ?> properties, boolean allowStructuralElements) {
        propertyWriter.handle(properties, writer);
        writer.print(System.lineSeparator());
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}