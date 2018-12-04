package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class EdgesWriterFactory implements WriterFactory<Path> {
    private final Directories directories;

    public EdgesWriterFactory(Directories directories) {
        this.directories = directories;
    }

    @Override
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata, Format format) throws IOException {

        java.nio.file.Path filePath = directories.createFilePath(directories.edgesDirectory(), name, index, format);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));

        Printer printer = format.createPrinter(printWriter, metadata);
        printer.printHeaderMandatoryColumns("~id","~label","~from","~to");

        return printer;
    }

    @Override
    public GraphElementHandler<Path> createLabelWriter(Printer printer) {
        return new EdgeWriter(printer);
    }
}
