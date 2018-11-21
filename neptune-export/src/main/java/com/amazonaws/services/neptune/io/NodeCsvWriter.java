package com.amazonaws.services.neptune.io;

import org.apache.tinkerpop.gremlin.structure.T;

import java.io.PrintWriter;
import java.util.Map;

public class NodeCsvWriter implements GraphElementHandler<Map<?, Object>> {

    private final PrintWriter writer;
    private final PropertyCsvWriter propertyCsvWriter;

    public NodeCsvWriter(PrintWriter writer, PropertyCsvWriter propertyCsvWriter) {
        this.writer = writer;
        this.propertyCsvWriter = propertyCsvWriter;
    }

    @Override
    public void handle(Map<?, Object> properties, boolean allowStructuralElements) {
        String id = String.valueOf(properties.get(T.id));
        String label = String.valueOf(properties.get(T.label));
        writer.printf("%s,%s", id, label) ;
        propertyCsvWriter.handle(properties, writer);
        writer.print(System.lineSeparator());
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
