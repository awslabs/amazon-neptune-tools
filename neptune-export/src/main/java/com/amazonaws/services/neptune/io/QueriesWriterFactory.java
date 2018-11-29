package com.amazonaws.services.neptune.io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Map;

public class QueriesWriterFactory implements WriterFactory<Map<?, ?>> {

    private final Directories directories;

    public QueriesWriterFactory(Directories directories) {
        this.directories = directories;
    }

    @Override
    public PrintWriter createPrinter(String name, int index) throws IOException {
        Path directory = directories.resultsDirectory().resolve(name);
        java.nio.file.Path csvFilePath = directories.createCsvFilePath(directory, name, index);
        return new PrintWriter(new FileWriter(csvFilePath.toFile()));
    }

    @Override
    public void printHeader(PrintWriter printer) {
        // Do nothing
    }

    @Override
    public GraphElementHandler<Map<?, ?>> createLabelWriter(PrintWriter printer, PropertyWriter propertyWriter) {
        return new QueryWriter(printer, propertyWriter);
    }
}
