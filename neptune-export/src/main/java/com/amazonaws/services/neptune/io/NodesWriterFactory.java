package com.amazonaws.services.neptune.io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class NodesWriterFactory implements WriterFactory<Map<?, Object>> {

    private final Directories directories;

    public NodesWriterFactory(Directories directories) {
        this.directories = directories;
    }

    @Override
    public PrintWriter createPrinter(String name, int index) throws IOException {
        java.nio.file.Path csvFilePath = directories.createCsvFilePath(directories.nodesDirectory(), name, index);
        return new PrintWriter(new FileWriter(csvFilePath.toFile()));
    }

    @Override
    public void printHeader(PrintWriter printer) {
        printer.print("~id,~label");
    }

    @Override
    public GraphElementHandler<Map<?, Object>> createLabelWriter(PrintWriter printer, PropertyCsvWriter propertyCsvWriter) {
        return new NodeCsvWriter(printer, propertyCsvWriter);
    }
}
