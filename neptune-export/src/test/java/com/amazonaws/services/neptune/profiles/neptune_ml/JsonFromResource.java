package com.amazonaws.services.neptune.profiles.neptune_ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class JsonFromResource {

    public static JsonNode get(String filename, Class<?> testClass) throws IOException {
        String path = String.format("%s/%s", testClass.getSimpleName(), filename);

        ClassLoader classLoader = testClass.getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(path)).getFile());

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(file);
    }
}
