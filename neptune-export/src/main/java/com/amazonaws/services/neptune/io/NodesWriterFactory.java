package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

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
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata, Format format) throws IOException {
        java.nio.file.Path filePath = directories.createFilePath(directories.nodesDirectory(), name, index, format);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));

        Printer printer = format.createPrinter(printWriter, metadata);
        printer.printHeaderMandatoryColumns("~id","~label");
        return printer;
    }

    @Override
    public GraphElementHandler<Map<?, Object>> createLabelWriter(Printer printer) {
        return new NodeWriter(printer);
    }
}
