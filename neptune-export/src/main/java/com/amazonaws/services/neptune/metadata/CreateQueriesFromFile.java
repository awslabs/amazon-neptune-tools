package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.graph.NamedQueriesCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.io.IOException;

public class CreateQueriesFromFile {
    private final File queriesFilePath;

    public CreateQueriesFromFile(File queriesFilePath) {
        this.queriesFilePath = queriesFilePath;
    }

    public NamedQueriesCollection execute() throws IOException {
        ArrayNode json = (ArrayNode) new ObjectMapper().readTree(queriesFilePath);
        return NamedQueriesCollection.fromJson(json);
    }
}
