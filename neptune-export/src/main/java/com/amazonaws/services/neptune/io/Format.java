package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public enum Format {
    json {
        @Override
        Printer createPrinter(PrintWriter writer, Map<String, PropertyTypeInfo> metadata) throws IOException {
            JsonGenerator generator = new JsonFactory().createGenerator(writer);
            generator.setPrettyPrinter(new MinimalPrettyPrinter(System.lineSeparator()));
            return new JsonPrinter(generator, metadata);
        }
    },
    csv {
        @Override
        Printer createPrinter(PrintWriter writer, Map<String, PropertyTypeInfo> metadata) {
            return new CsvPrinter(writer, metadata);
        }
    };

    abstract Printer createPrinter(PrintWriter writer, Map<String, PropertyTypeInfo> metadata) throws IOException;

    public String description(){
        return name().toUpperCase();
    }

}
