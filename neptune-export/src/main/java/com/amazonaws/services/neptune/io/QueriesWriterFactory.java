package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;

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
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata, Format format) throws IOException {
        Path directory = directories.resultsDirectory().resolve(name);
        java.nio.file.Path filePath = directories.createFilePath(directory, name, index, format);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));

        return format.createPrinter(printWriter, metadata);
    }

    @Override
    public GraphElementHandler<Map<?, ?>> createLabelWriter(Printer printer) {
        return new QueryWriter(printer);
    }
}
