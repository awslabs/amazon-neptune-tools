package com.amazonaws.services.neptune.io;

import org.apache.tinkerpop.gremlin.structure.T;

import java.io.IOException;
import java.util.Map;

public class NodeWriter implements GraphElementHandler<Map<?, Object>> {

    private final Printer printer;

    public NodeWriter(Printer printer) {
        this.printer = printer;
    }


    @Override
    public void handle(Map<?, Object> properties, boolean allowStructuralElements) throws IOException {
        String id = String.valueOf(properties.get(T.id));
        String label = String.valueOf(properties.get(T.label));

        printer.printStartRow();
        printer.printNode(id, label);
        printer.printProperties(properties);
        printer.printEndRow();
    }

    @Override
    public void close() throws Exception {
        printer.close();
    }
}
