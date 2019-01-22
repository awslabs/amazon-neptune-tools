package com.amazonaws.services.neptune.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public enum DirectoryStructure {
    Config {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(directory);
        }
    },
    PropertyGraph {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(nodesDirectory);
            Files.createDirectories(edgesDirectory);
        }
    },
    Rdf {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(statementsDirectory);
        }
    },
    GremlinQueries {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(resultsDirectory);
        }
    },
    SparqlQueries {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(resultsDirectory);
        }
    };

    public abstract void createDirectories(Path directory,
                                           Path nodesDirectory,
                                           Path edgesDirectory,
                                           Path statementsDirectory,
                                           Path resultsDirectory) throws IOException;

}
