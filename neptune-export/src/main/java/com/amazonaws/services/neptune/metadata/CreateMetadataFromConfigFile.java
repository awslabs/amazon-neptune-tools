package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class CreateMetadataFromConfigFile implements MetadataCommand {
    private final File configFilePath;

    public CreateMetadataFromConfigFile(File configFilePath) {
        this.configFilePath = configFilePath;
    }

    @Override
    public PropertiesMetadataCollection execute() throws IOException {
        JsonNode json = new ObjectMapper().readTree(configFilePath);
        return PropertiesMetadataCollection.fromJson(json);
    }
}
