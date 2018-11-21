package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.*;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SaveMetadataConfig {
    private final PropertiesMetadataCollection metadataCollection;
    private final Path configFilePath;

    public SaveMetadataConfig(PropertiesMetadataCollection metadataCollection, Path configFilePath) {
        this.metadataCollection = metadataCollection;
        this.configFilePath = configFilePath;
    }

    public void execute() throws IOException {
        File file = configFilePath.toFile();
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF_8))) {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            writer.write(objectWriter.writeValueAsString(metadataCollection.toJson()));
        }
    }
}
