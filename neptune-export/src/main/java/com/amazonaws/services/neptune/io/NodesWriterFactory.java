package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;

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
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata) throws IOException {
        java.nio.file.Path filePath = directories.createCsvFilePath(directories.nodesDirectory(), name, index);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));
        CsvPrinter printer = new CsvPrinter(printWriter, metadata);
        printer.printHeaderMandatoryColumns("~id","~label");
        return printer;
    }

    @Override
    public GraphElementHandler<Map<?, Object>> createLabelWriter(Printer printer) {
        return new NodeWriter(printer);
    }
}
