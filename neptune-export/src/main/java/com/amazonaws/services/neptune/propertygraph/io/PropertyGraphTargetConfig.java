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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.*;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

public class PropertyGraphTargetConfig {

    private final Directories directories;
    private final PropertyGraphExportFormat format;
    private final Target output;
    private final boolean includeTypeDefinitions;
    private final KinesisConfig kinesisConfig;
    private final boolean inferSchema;
    private final boolean mergeFiles;

    public PropertyGraphTargetConfig(Directories directories,
                                     KinesisConfig kinesisConfig,
                                     boolean includeTypeDefinitions,
                                     PropertyGraphExportFormat format,
                                     Target output,
                                     boolean inferSchema,
                                     boolean mergeFiles) {
        this.directories = directories;
        this.format = format;
        this.output = output;
        this.includeTypeDefinitions = includeTypeDefinitions;
        this.kinesisConfig = kinesisConfig;
        this.inferSchema = inferSchema;
        this.mergeFiles = mergeFiles;
    }

    public Target output() {
        return output;
    }

    public PropertyGraphExportFormat format() {
        return format;
    }

    public boolean mergeFiles() {
        return mergeFiles;
    }

    public PropertyGraphPrinter createPrinterForQueries(String name, LabelSchema labelSchema) throws IOException {
        return createPrinterForQueries(() -> directories.createQueryResultsFilePath(name, fileExtension(useTempFiles())), labelSchema);
    }

    private PropertyGraphPrinter createPrinterForQueries(Supplier<Path> pathSupplier, LabelSchema labelSchema) throws IOException {
        OutputWriter outputWriter = output.createOutputWriter(pathSupplier, kinesisConfig);
        return createPrinter(labelSchema, outputWriter);
    }

    public PropertyGraphPrinter createPrinterForEdges(String name, LabelSchema labelSchema) throws IOException {
        return createPrinterForEdges(() -> directories.createEdgesFilePath(name, fileExtension(useTempFiles())), labelSchema);
    }

    private PropertyGraphPrinter createPrinterForEdges(Supplier<Path> pathSupplier, LabelSchema labelSchema) throws IOException {
        OutputWriter outputWriter = output.createOutputWriter(pathSupplier, kinesisConfig);
        return createPrinter(labelSchema, outputWriter);
    }

    public PropertyGraphPrinter createPrinterForNodes(String name, LabelSchema labelSchema) throws IOException {
        return createPrinterForNodes(() -> directories.createNodesFilePath(name, fileExtension(useTempFiles())), labelSchema);
    }

    private PropertyGraphPrinter createPrinterForNodes(Supplier<Path> pathSupplier, LabelSchema labelSchema) throws IOException {
        OutputWriter outputWriter = output.createOutputWriter(pathSupplier, kinesisConfig);
        return createPrinter(labelSchema, outputWriter);
    }

    public PropertyGraphTargetConfig forFileConsolidation(){
        return new PropertyGraphTargetConfig(directories, kinesisConfig, includeTypeDefinitions, format, output, false, mergeFiles);
    }

    private boolean useTempFiles(){
        return !inferSchema;
    }

    private PropertyGraphPrinter createPrinter(LabelSchema labelSchema, OutputWriter outputWriter) throws IOException {
        if (inferSchema){
            return format.createPrinterForInferredSchema(outputWriter, labelSchema, includeTypeDefinitions);
        } else {
            return format.createPrinter(outputWriter, labelSchema, includeTypeDefinitions);
        }
    }

    private FileExtension fileExtension(boolean tempFile) {
        return tempFile ? FileExtension.TEMP_FILE : format;
    }

    public RewriteCommand createRewriteCommand() {
        return format.createRewriteCommand(this);
    }
}
