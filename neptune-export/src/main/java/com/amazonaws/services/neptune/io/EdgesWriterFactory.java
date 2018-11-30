package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
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
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata) throws IOException {
        java.nio.file.Path filePath = directories.createCsvFilePath(directories.edgesDirectory(), name, index);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));
        CsvPrinter printer = new CsvPrinter(printWriter, metadata);
        printer.printHeaderMandatoryColumns("~id","~label","~from","~to");
        return printer;
    }

    @Override
    public GraphElementHandler<Path> createLabelWriter(Printer printer) {
        return new EdgeWriter(printer);
    }
}
