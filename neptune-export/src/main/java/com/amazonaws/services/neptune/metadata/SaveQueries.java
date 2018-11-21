package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.graph.NamedQueriesCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.*;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SaveQueries {
    private final NamedQueriesCollection namedQueries;
    private final Path queriesFilePath;

    public SaveQueries(NamedQueriesCollection namedQueries, Path queriesFilePath) {
        this.namedQueries = namedQueries;
        this.queriesFilePath = queriesFilePath;
    }

    public void execute() throws IOException {
        File file = queriesFilePath.toFile();
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF_8))) {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            writer.write(objectWriter.writeValueAsString(namedQueries.toJson()));
        }
    }
}
