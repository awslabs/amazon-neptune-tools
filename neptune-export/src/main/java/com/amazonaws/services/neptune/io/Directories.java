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

import com.amazonaws.services.neptune.cluster.EventId;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.NamedQueriesCollection;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Directories {

    public static String fileName(String name, AtomicInteger index) throws UnsupportedEncodingException {
        String filename = String.format("%s-%s", name, index.incrementAndGet());
        return URLEncoder.encode(filename, StandardCharsets.UTF_8.toString());
    }

    public static String fileName(String filename) throws UnsupportedEncodingException {
        return URLEncoder.encode(filename, StandardCharsets.UTF_8.toString());
    }

    private static final String CONFIG_FILE = "config.json";
    private static final String LAST_EVENT_ID_FILE = "lastEventId.json";
    private static final String QUERIES_FILE = "queries.json";

    public static Directories createFor(DirectoryStructure directoryStructure,
                                        File root,
                                        String exportId,
                                        String tag,
                                        String partitionDirectories) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("You must supply a directory");
        }

        Path rootDirectory = root.toPath();
        Path directory;

        if (StringUtils.isNotEmpty(partitionDirectories)){
            directory = rootDirectory;
        } else {
            String directoryName = tag.isEmpty() ?
                    exportId :
                    String.format("%s-%s", tag, exportId);

            directory = rootDirectory.resolve(directoryName);
        }

        Path nodesDirectory = createElementDirectory("nodes", directory, partitionDirectories);
        Path edgesDirectory = createElementDirectory("edges", directory, partitionDirectories);
        Path statementsDirectory = createElementDirectory("statements", directory, partitionDirectories);
        Path resultsDirectory = createElementDirectory("results", directory, partitionDirectories);
        Path recordsDirectory = createElementDirectory("records", directory, partitionDirectories);

        directoryStructure.createDirectories(
                directory,
                nodesDirectory,
                edgesDirectory,
                statementsDirectory,
                resultsDirectory,
                recordsDirectory);

        return new Directories(
                directory,
                pathOrNull(nodesDirectory),
                pathOrNull(edgesDirectory),
                pathOrNull(statementsDirectory),
                resultsDirectory,
                pathOrNull(recordsDirectory),
                tag);
    }

    private static Path pathOrNull(Path path){
        if (path.toFile().exists()){
            return path;
        } else {
            return null;
        }
    }

    private static Path createElementDirectory(String name, Path directory, String partitionDirectories){
        Path elementDirectory = directory.resolve(name);
        if (StringUtils.isNotEmpty(partitionDirectories)){
            String[] partitions = partitionDirectories.split("/");
            for (String partition : partitions) {
                if (StringUtils.isNotEmpty(partition)){
                    elementDirectory = elementDirectory.resolve(partition);
                }
            }
        }
        return elementDirectory;
    }

    private final String tag;
    private final Path directory;
    private final Path nodesDirectory;
    private final Path edgesDirectory;
    private final Path statementsDirectory;
    private final Path resultsDirectory;
    private final Path recordsDirectory;

    private Directories(Path directory,
                        Path nodesDirectory,
                        Path edgesDirectory,
                        Path statementsDirectory,
                        Path resultsDirectory,
                        Path recordsDirectory,
                        String tag) {
        this.directory = directory;
        this.nodesDirectory = nodesDirectory;
        this.edgesDirectory = edgesDirectory;
        this.statementsDirectory = statementsDirectory;
        this.resultsDirectory = resultsDirectory;
        this.recordsDirectory = recordsDirectory;
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

    public Path rootDirectory() {
        return directory.toAbsolutePath();
    }

    public Collection<Path> subdirectories(){
        List<Path> paths = new ArrayList<>();
        addIfNotNull(nodesDirectory, paths);
        addIfNotNull(edgesDirectory, paths);
        addIfNotNull(statementsDirectory, paths);
        addIfNotNull(resultsDirectory, paths);
        addIfNotNull(recordsDirectory, paths);
        return paths;
    }

    private void addIfNotNull(Path path, List<Path> paths){
        if (path != null){
            paths.add(path.toAbsolutePath());
        }
    }

    public Path writeConfigFilePathAsReturnValue(CommandWriter writer){
        Path path = configFilePath().toAbsolutePath();
        writer.writeReturnValue(path.toString());
        return path;
    }

    public void writeResultsDirectoryPathAsMessage(String fileType, CommandWriter writer){
        writer.writeMessage(fileType + " files : " + resultsDirectory.toAbsolutePath().toString());
    }

    public Path createNodesFilePath(String name, FileExtension extension, Label label, boolean perLabelDirectories)  {

        if (nodesDirectory == null && recordsDirectory != null){
            return createFilePath(recordsDirectory, String.format("nodes-%s", name), extension);
        } else if (perLabelDirectories){
            File labelDirectory = new File(nodesDirectory.toFile(), label.labelsAsString());
            if (!labelDirectory.exists()){
                synchronized(this){
                    if (!labelDirectory.exists()){
                        try {
                            Files.createDirectories(labelDirectory.toPath());
                        } catch (IOException e) {
                            throw new RuntimeException(String.format("Unable to create nodes directory for %s", label.labelsAsString()));
                        }
                    }
                }
            }
            return createFilePath(labelDirectory.toPath(), name, extension);
        } else {
            return createFilePath(nodesDirectory, name, extension);
        }
    }

    public Path createEdgesFilePath(String name, FileExtension extension, Label label, boolean perLabelDirectories){
        if (edgesDirectory == null && recordsDirectory != null){
            return createFilePath(recordsDirectory, String.format("edges-%s", name), extension);
        }
        if (perLabelDirectories){
            File labelDirectory = new File(edgesDirectory.toFile(), label.labelsAsString());
            if (!labelDirectory.exists()){
                synchronized(this){
                    if (!labelDirectory.exists()){
                        try {
                            Files.createDirectories(labelDirectory.toPath());
                        } catch (IOException e) {
                            throw new RuntimeException(String.format("Unable to create edges directory for %s", label.labelsAsString()));
                        }
                    }
                }
            }
            return createFilePath(labelDirectory.toPath(), name, extension);
        } else {
            return createFilePath(edgesDirectory, name, extension);
        }
    }

    public Path createStatementsFilePath(String name, FileExtension extension){
        if (statementsDirectory == null && recordsDirectory != null){
            return createFilePath(recordsDirectory, name, extension);
        } else {
            return createFilePath(statementsDirectory, name, extension);
        }
    }

    public Path createQueryResultsFilePath(String directoryName, String fileName, FileExtension extension){
        Path directory = resultsDirectory.resolve(directoryName);
        return createFilePath(directory, fileName, extension);
    }

    public void createResultsSubdirectories(Collection<String> subdirectoryNames) throws IOException {

        for (String subdirectoryName : subdirectoryNames) {
            Files.createDirectories(resultsDirectory.resolve(subdirectoryName));
        }
    }

    public JsonResource<GraphSchema> configFileResource() {
        return new JsonResource<>("Config file",
                configFilePath().toUri(),
                GraphSchema.class);
    }

    public JsonResource<EventId> lastEventIdFileResource() {
        return new JsonResource<>("LastEventId file",
                lastEventIdFilePath().toUri(),
                EventId.class);
    }

    public JsonResource<NamedQueriesCollection> queriesResource() {
        return new JsonResource<>("Queries file",
                queriesFilePath().toUri(),
                NamedQueriesCollection.class);
    }

    public Path createFilePath(Path directory, String name, FileExtension extension) {

        String filenameWithoutExtension = tag.isEmpty() ?
                name :
                String.format("%s-%s", tag, name);

        String filename = filenameWithoutExtension.getBytes().length > 250 ?
                String.format("%s.%s", DigestUtils.sha1Hex(filenameWithoutExtension), extension.extension()) :
                String.format("%s.%s", filenameWithoutExtension, extension.extension());

        return directory.resolve(filename);
    }

    private Path configFilePath() {
        return directory.resolve(CONFIG_FILE).toAbsolutePath();
    }

    private Path lastEventIdFilePath() {
        return directory.resolve(LAST_EVENT_ID_FILE).toAbsolutePath();
    }

    private Path queriesFilePath() {
        return directory.resolve(QUERIES_FILE).toAbsolutePath();
    }

}
