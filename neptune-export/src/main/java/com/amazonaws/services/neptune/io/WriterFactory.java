package com.amazonaws.services.neptune.io;

import java.io.IOException;
import java.io.PrintWriter;

public interface WriterFactory<T> {

    PrintWriter createPrinter(String name, int index) throws IOException;

    void printHeader(PrintWriter printer);

    GraphElementHandler<T> createLabelWriter(PrintWriter printer, PropertyCsvWriter propertyCsvWriter);
}
