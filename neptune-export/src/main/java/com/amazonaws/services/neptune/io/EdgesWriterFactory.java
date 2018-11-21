package com.amazonaws.services.neptune.io;

import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class EdgesWriterFactory implements WriterFactory<Path> {
    private final Directories directories;

    public EdgesWriterFactory(Directories directories) {
        this.directories = directories;
    }

    @Override
    public PrintWriter createPrinter(String name, int index) throws IOException {
        java.nio.file.Path csvFilePath = directories.createCsvFilePath(directories.edgesDirectory(), name, index);
        return new PrintWriter(new FileWriter(csvFilePath.toFile()));
    }

    @Override
    public void printHeader(PrintWriter printer) {
        printer.print("~id,~label,~from,~to");
    }

    @Override
    public GraphElementHandler<Path> createLabelWriter(PrintWriter printer, PropertyCsvWriter propertyCsvWriter) {
        return new EdgeCsvWriter(printer, propertyCsvWriter);
    }
}
