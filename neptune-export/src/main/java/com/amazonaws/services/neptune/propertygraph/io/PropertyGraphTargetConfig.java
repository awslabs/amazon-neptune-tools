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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.KinesisConfig;
import com.amazonaws.services.neptune.io.Target;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

public class PropertyGraphTargetConfig {

    private final Directories directories;
    private final PropertyGraphExportFormat format;
    private final Target output;
    private final boolean includeTypeDefinitions;
    private final KinesisConfig kinesisConfig;

    public PropertyGraphTargetConfig(Directories directories,
                                     KinesisConfig kinesisConfig,
                                     boolean includeTypeDefinitions,
                                     PropertyGraphExportFormat format,
                                     Target output) {
        this.directories = directories;
        this.format = format;
        this.output = output;
        this.includeTypeDefinitions = includeTypeDefinitions;
        this.kinesisConfig = kinesisConfig;
    }

    public String formatDescription() {
        return format.description();
    }

    public String outputDescription() {
        return output.name();
    }

    public PropertyGraphPrinter createPrinterForQueries(String name, int index, Map<Object, PropertyTypeInfo> metadata) throws IOException {

        OutputWriter outputWriter = output.createOutputWriter(
                () -> directories.createQueryResultsFilePath(name, index, format),
                kinesisConfig);

        return format.createPrinter(outputWriter, metadata, includeTypeDefinitions);
    }

    public PropertyGraphPrinter createPrinterForEdges(String name, int index, Map<Object, PropertyTypeInfo> metadata) throws IOException {

        OutputWriter outputWriter = output.createOutputWriter(
                () -> directories.createEdgesFilePath(name, index, format),
                kinesisConfig);

        return format.createPrinter(outputWriter, metadata, includeTypeDefinitions);
    }

    public PropertyGraphPrinter createPrinterForNodes(String name, int index, Map<Object, PropertyTypeInfo> metadata) throws IOException {

        OutputWriter outputWriter = output.createOutputWriter(
                () -> directories.createNodesFilePath(name, index, format),
                kinesisConfig);

        return format.createPrinter(outputWriter, metadata, includeTypeDefinitions);
    }
}
