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

import com.amazonaws.services.neptune.propertygraph.NamedQueriesCollection;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadataCollection;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.UUID;

public class Directories {

    private static final String CONFIG_FILE = "config.json";
    private static final String QUERIES_FILE = "queries.json";

    public static Directories createFor(DirectoryStructure directoryStructure, File root, String tag) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("You must supply a directory");
        }

        String uuid = UUID.randomUUID().toString().replace("-", "");

        String directoryName = tag.isEmpty() ?
                uuid :
                String.format("%s-%s", tag, uuid);
        Path rootDirectory = root.toPath();

        Path directory = rootDirectory.resolve(directoryName);
        Path nodesDirectory = directory.resolve("nodes");
        Path edgesDirectory = directory.
                resolve("edges");
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

    public void writeRootDirectoryPathAsMessage(String fileType, CommandWriter writer){
        writer.writeMessage(fileType + " files : " + directory.toAbsolutePath().toString());
    }

    public Path writeRootDirectoryPathAsReturnValue(CommandWriter writer){
        Path path = directory.toAbsolutePath();
        writer.writeReturnValue(path.toString());
        return path;
    }

    public Path writeConfigFilePathAsReturnValue(CommandWriter writer){
        Path path = configFilePath().toAbsolutePath();
        writer.writeReturnValue(path.toString());
        return path;
    }

    public void writeResultsDirectoryPathAsMessage(String fileType, CommandWriter writer){
        writer.writeMessage(fileType + " files : " + resultsDirectory.toAbsolutePath().toString());
    }

    public Path createNodesFilePath(String name, int index, FileExtension extension){
        return createFilePath(nodesDirectory, name, index, extension);
    }

    public Path createEdgesFilePath(String name, int index, FileExtension extension){
        return createFilePath(edgesDirectory, name, index, extension);
    }

    public Path createStatementsFilePath(String name, int index, FileExtension extension){
        return createFilePath(statementsDirectory, name, index, extension);
    }

    public Path createQueryResultsFilePath(String name, int index, FileExtension extension){
        Path directory = resultsDirectory.resolve(name);
        return createFilePath(directory, name, index, extension);
    }

    public void createResultsSubdirectories(Collection<String> subdirectoryNames) throws IOException {
        for (String subdirectoryName : subdirectoryNames) {
            Files.createDirectories(resultsDirectory.resolve(subdirectoryName));
        }
    }

    public JsonResource<PropertiesMetadataCollection> configFileResource() {
        return new JsonResource<>("Config file",
                configFilePath().toUri(),
                PropertiesMetadataCollection.class);
    }

    public JsonResource<NamedQueriesCollection> queriesResource() {
        return new JsonResource<>("Queries file",
                queriesFilePath().toUri(),
                NamedQueriesCollection.class);
    }

    private Path createFilePath(Path directory, String name, int index, FileExtension extension) {
        String filename = tag.isEmpty() ?
                String.format("%s-%s.%s", name, index, extension.suffix()) :
                String.format("%s-%s-%s.%s", tag, name, index, extension.suffix());
        return directory.resolve(filename);
    }

    private Path configFilePath() {
        return directory.resolve(CONFIG_FILE).toAbsolutePath();
    }

    private Path queriesFilePath() {
        return directory.resolve(QUERIES_FILE).toAbsolutePath();
    }

}
