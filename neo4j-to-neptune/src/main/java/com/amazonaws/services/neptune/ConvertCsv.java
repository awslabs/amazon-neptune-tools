/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.Neo4jStreamWriter;
import com.amazonaws.services.neptune.io.OutputFile;
import com.amazonaws.services.neptune.metadata.*;
import com.amazonaws.services.neptune.util.CSVUtils;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;

@Command(name = "convert-csv", description = "Converts CSV file exported from Neo4j via 'apoc.export.csv.all' to Neptune Gremlin import CSV files")
public class ConvertCsv implements Runnable {

    @Option(name = {"-d", "--dir"}, description = "Root directory for output")
    @Required
    @Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File outputDirectory;

    @Option(name = {"-df", "--dotenv-file"}, description = "Path to the Dotenv file for Neo4j AuraDB connection.")
    @RequireOnlyOne(tag = "input")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File envFile;

    @Option(name = {"-u", "--uri"}, description = "URI of the Neo4j Database to stream data from")
    @RequireOnlyOne(tag = "input")
    @Once
    private String uri;

    @Option(name = {"-n", "--username"}, description = "Neo4j Database username")
    @Once
    private String username;

    @Option(name = {"-pw", "--password"}, description = "Neo4j Database password")
    @Once
    private String password;

    @Option(name = {"-i", "--input"}, description = "Path to Neo4j CSV file")
    @RequireOnlyOne(tag = "input")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File inputFile;

    @Option(name = {"--conversion-config"}, description = "Path to YAML file containing configuration for label mappings and record filtering")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File conversionConfigFile;

    @Option(name = {"--node-property-policy"}, description = "Conversion policy for multi-valued node properties (default, 'PutInSetIgnoringDuplicates')")
    @Once
    @AllowedValues(allowedValues = {"LeaveAsString", "Halt", "PutInSetIgnoringDuplicates", "PutInSetButHaltIfDuplicates"})
    private MultiValuedNodePropertyPolicy multiValuedNodePropertyPolicy = MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates;

    @Option(name = {"--relationship-property-policy"}, description = "Conversion policy for multi-valued relationship properties (default, 'LeaveAsString')")
    @Once
    @AllowedValues(allowedValues = {"LeaveAsString", "Halt"})
    private MultiValuedRelationshipPropertyPolicy multiValuedRelationshipPropertyPolicy = MultiValuedRelationshipPropertyPolicy.LeaveAsString;

    @Option(name = {"--semi-colon-replacement"}, description = "Replacement for semi-colon character in multi-value string properties (default, ' ')")
    @Once
    @Pattern(pattern = "^[^;]*$", description = "Replacement string cannot contain a semi-colon.")
    private String semiColonReplacement = " ";

    @Option(name = {"--infer-types"}, description = "Infer data types for CSV column headings")
    @Once
    private boolean inferTypes = false;

    @Override
    public void run() {
        try {
            // Load label mapping configuration
            ConversionConfig conversionConfig = ConversionConfig.fromFile(conversionConfigFile);

            Directories directories = Directories.createFor(outputDirectory);

            File input = inputFile;
            File tempDataFile = null;

            // if no input file provided, it is via streaming
            if (input == null) {
                String uriInput, usernameInput, passwordInput;
                if (envFile != null) {
                    Dotenv dotenv = Dotenv.configure()
                            .directory(envFile.getParent())
                            .filename(envFile.getName())
                            .load();
                    uriInput = dotenv.get("NEO4J_URI");
                    usernameInput = dotenv.get("NEO4J_USERNAME");
                    passwordInput = dotenv.get("NEO4J_PASSWORD");
                } else {
                    uriInput = uri;
                    usernameInput = username;
                    passwordInput = password;
                }
                try (Neo4jStreamWriter writer = new Neo4jStreamWriter(uriInput, usernameInput, passwordInput, directories)) {
                    tempDataFile = writer.streamToFile();
                }

                input = tempDataFile;
            }

            try (Timer timer = new Timer();
                 OutputFile vertexFile = new OutputFile(directories, "vertices");
                 OutputFile edgeFile = new OutputFile(directories, "edges");
                 CSVParser parser = CSVUtils.newParser(input)) {

                Iterator<CSVRecord> iterator = parser.iterator();

                final AtomicLong vertexCount = new AtomicLong(0);
                final AtomicLong edgeCount = new AtomicLong(0);
                final AtomicLong skippedVertexCount = new AtomicLong(0);
                final AtomicLong skippedEdgeCount = new AtomicLong(0);

                if (iterator.hasNext()) {
                    CSVRecord headers = iterator.next();

                    VertexMetadata vertexMetadata = VertexMetadata.parse(
                            headers,
                            new PropertyValueParser(multiValuedNodePropertyPolicy, semiColonReplacement, inferTypes),
                            conversionConfig);
                    EdgeMetadata edgeMetadata = EdgeMetadata.parse(
                            headers,
                            new PropertyValueParser(multiValuedRelationshipPropertyPolicy, semiColonReplacement, inferTypes),
                            conversionConfig, vertexMetadata.getSkippedVertexIds());

                    while (iterator.hasNext()) {
                        CSVRecord record = iterator.next();
                        if (vertexMetadata.isVertex(record)) {
                            vertexMetadata.toIterable(record).ifPresentOrElse(it -> {
                                try {
                                    vertexFile.printRecord(it);
                                    vertexCount.getAndIncrement();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }, skippedVertexCount::getAndIncrement);
                        } else if (edgeMetadata.isEdge(record)) {
                            edgeMetadata.toIterable(record).ifPresentOrElse(it -> {
                                try {
                                    edgeFile.printRecord(it);
                                    edgeCount.getAndIncrement();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }, skippedEdgeCount::getAndIncrement);
                        } else {
                            throw new IllegalStateException("Unable to parse record: " + record.toString());
                        }
                    }

                    vertexFile.printHeaders(vertexMetadata.headers());
                    edgeFile.printHeaders(edgeMetadata.headers());

                    System.err.println("Vertices: " + vertexCount);
                    System.err.println("Edges   : " + edgeCount);

                    if (conversionConfig.hasSkipRules()) {
                        System.err.println("Skipped vertices: " + skippedVertexCount);
                        System.err.println("Skipped edges  : " + skippedEdgeCount);
                        System.err.println("Skip rules: " +
                                (!conversionConfig.getSkipVertices().getById().isEmpty() ?
                                        conversionConfig.getSkipVertices().getById().size() + " vertex IDs, " : "") +
                                (!conversionConfig.getSkipVertices().getByLabel().isEmpty() ?
                                        conversionConfig.getSkipVertices().getByLabel().size() + " vertex labels, " : "") +
                                (!conversionConfig.getSkipEdges().getByLabel().isEmpty()  ?
                                        conversionConfig.getSkipEdges().getByLabel().size() + " edge labels" : ""));
                    }

                    System.err.println("Output  : " + directories.outputDirectory());

                    if (!conversionConfig.getVertexLabels().isEmpty() || !conversionConfig.getEdgeLabels().isEmpty()) {
                        System.err.println("Label mappings applied from: " + conversionConfigFile.getAbsolutePath());
                    }

                    System.out.println(directories.outputDirectory());

                    // delete streamed data file after conversion
                    if (tempDataFile != null) Files.deleteIfExists(tempDataFile.toPath());
                }

            }


        } catch (Exception e) {
            System.err.println("An error occurred while converting Neo4j CSV file:");
            e.printStackTrace();
        }
    }
}
