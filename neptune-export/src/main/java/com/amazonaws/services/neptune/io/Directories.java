package com.amazonaws.services.neptune.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

public class Directories {

    private static final String CONFIG_FILE = "config.json";
    private static final String QUERIES_FILE = "queries.json";

    public static Directories createFor(File root, String tag) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("You must supply a directory");
        }

        String directoryName = tag.isEmpty() ?
                String.valueOf(System.currentTimeMillis()) :
                String.format("%s-%s", tag, System.currentTimeMillis());
        Path rootDirectory = root.toPath();
        Path directory = rootDirectory.resolve(directoryName);
        Path nodesDirectory = directory.resolve("nodes");
        Path edgesDirectory = directory.resolve("edges");
        Path resultsDirectory = directory.resolve("results");

        Files.createDirectories(nodesDirectory);
        Files.createDirectories(edgesDirectory);
        Files.createDirectories(resultsDirectory);

        return new Directories(directory, nodesDirectory, edgesDirectory, resultsDirectory, tag);
    }

    private final String tag;
    private final Path directory;
    private final Path nodesDirectory;
    private final Path edgesDirectory;
    private final Path resultsDirectory;

    private Directories(Path directory, Path nodesDirectory, Path edgesDirectory, Path resultsDirectory, String tag) {
        this.directory = directory;
        this.nodesDirectory = nodesDirectory;
        this.edgesDirectory = edgesDirectory;
        this.resultsDirectory = resultsDirectory;
        this.tag = tag;
    }

    public Path directory() {
        return directory.toAbsolutePath();
    }

    public Path nodesDirectory() {
        return nodesDirectory.toAbsolutePath();
    }

    public Path edgesDirectory() {
        return edgesDirectory.toAbsolutePath();
    }

    public Path resultsDirectory() {
        return resultsDirectory.toAbsolutePath();
    }

    public Path configFilePath() {
        return directory.resolve(CONFIG_FILE).toAbsolutePath();
    }

    public Path queriesFilePath() {
        return directory.resolve(QUERIES_FILE).toAbsolutePath();
    }

    public Path createFilePath(Path directory, String name, int index, Format format) {
        String filename = tag.isEmpty() ?
                String.format("%s-%s.%s", name, index, format.name()) :
                String.format("%s-%s-%s.%s", tag, name, index, format.name());
        return directory.resolve(filename);
    }

    public void createSubdirectories(Path parentDirectory, Collection<String> subdirectoryNames) throws IOException {
        for (String subdirectoryName : subdirectoryNames) {
            Files.createDirectories(parentDirectory.resolve(subdirectoryName));
        }
    }
}
