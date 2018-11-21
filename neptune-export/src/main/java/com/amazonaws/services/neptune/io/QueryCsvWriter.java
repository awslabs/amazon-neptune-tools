package com.amazonaws.services.neptune.io;

import java.io.PrintWriter;
import java.util.Map;

public class QueryCsvWriter implements GraphElementHandler<Map<?, ?>> {

    private final PrintWriter writer;
    private final PropertyCsvWriter propertyCsvWriter;

    public QueryCsvWriter(PrintWriter writer, PropertyCsvWriter propertyCsvWriter) {
        this.writer = writer;
        this.propertyCsvWriter = propertyCsvWriter;
    }

    @Override
    public void handle(Map<?, ?> properties, boolean allowStructuralElements) {
        propertyCsvWriter.handle(properties, writer);
        writer.print(System.lineSeparator());
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}