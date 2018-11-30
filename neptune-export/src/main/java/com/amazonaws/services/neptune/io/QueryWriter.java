package com.amazonaws.services.neptune.io;

import java.util.Map;

public class QueryWriter implements GraphElementHandler<Map<?, ?>> {

    private final Printer printer;

    public QueryWriter(Printer printer) {
        this.printer = printer;
    }

    @Override
    public void handle(Map<?, ?> properties, boolean allowStructuralElements) {

        printer.printStartRow();
        printer.printProperties(properties);
        printer.printEndRow();
    }

    @Override
    public void close() throws Exception {
        printer.close();
    }
}