/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

public class Directories {

    private static final String CONFIG_FILE = "config.json";
    private static final String QUERIES_FILE = "queries.json";

    public static Directories createFor(DirectoryStructure directoryStructure, File root, String tag) throws IOException {
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
        Path statementsDirectory = directory.resolve("statements");
        Path resultsDirectory = directory.resolve("results");

        directoryStructure.createDirectories(directory, nodesDirectory, edgesDirectory, statementsDirectory, resultsDirectory);

        return new Directories(directory, nodesDirectory, edgesDirectory, statementsDirectory, resultsDirectory, tag);
    }

    private final String tag;
    private final Path directory;
    private final Path nodesDirectory;
    private final Path edgesDirectory;
    private final Path statementsDirectory;
    private final Path resultsDirectory;

    private Directories(Path directory, Path nodesDirectory, Path edgesDirectory, Path statementsDirectory, Path resultsDirectory, String tag) {
        this.directory = directory;
        this.nodesDirectory = nodesDirectory;
        this.edgesDirectory = edgesDirectory;
        this.statementsDirectory = statementsDirectory;
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

    public Path statementsDirectory() {
        return statementsDirectory.toAbsolutePath();
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

    public Path createFilePath(Path directory, String name, int index, FileExtension extension) {
        String filename = tag.isEmpty() ?
                String.format("%s-%s.%s", name, index, extension.name()) :
                String.format("%s-%s-%s.%s", tag, name, index, extension.name());
        return directory.resolve(filename);
    }

    public void createSubdirectories(Path parentDirectory, Collection<String> subdirectoryNames) throws IOException {
        for (String subdirectoryName : subdirectoryNames) {
            Files.createDirectories(parentDirectory.resolve(subdirectoryName));
        }
    }
}
