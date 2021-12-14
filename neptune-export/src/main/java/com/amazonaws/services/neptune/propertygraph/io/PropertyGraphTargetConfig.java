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

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.KinesisConfig;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.io.Target;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.MasterLabelSchemas;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

public class PropertyGraphTargetConfig {

    private final Directories directories;
    private final KinesisConfig kinesisConfig;
    private final PrinterOptions printerOptions;
    private final boolean inferSchema;
    private final PropertyGraphExportFormat format;
    private final Target output;
    private final boolean mergeFiles;
    private final boolean perLabelDirectories;

    public PropertyGraphTargetConfig(Directories directories,
                                     KinesisConfig kinesisConfig,
                                     PrinterOptions printerOptions,
                                     PropertyGraphExportFormat format,
                                     Target output,
                                     boolean mergeFiles,
                                     boolean perLabelDirectories,
                                     boolean inferSchema) {
        this.directories = directories;
        this.kinesisConfig = kinesisConfig;
        this.printerOptions = printerOptions;
        this.inferSchema = inferSchema;
        this.format = format;
        this.output = output;
        this.mergeFiles = mergeFiles;
        this.perLabelDirectories = perLabelDirectories;
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
        return createPrinterForQueries(() -> directories.createQueryResultsFilePath(labelSchema.label().labelsAsString(), name, format), labelSchema);
    }

    private PropertyGraphPrinter createPrinterForQueries(Supplier<Path> pathSupplier, LabelSchema labelSchema) throws IOException {
        OutputWriter outputWriter = output.createOutputWriter(pathSupplier, kinesisConfig);
        return createPrinter(labelSchema, outputWriter);
    }

    public PropertyGraphPrinter createPrinterForEdges(String name, LabelSchema labelSchema) throws IOException {
        return createPrinterForEdges(() -> directories.createEdgesFilePath(name, format, labelSchema.label(), perLabelDirectories), labelSchema);
    }

    private PropertyGraphPrinter createPrinterForEdges(Supplier<Path> pathSupplier, LabelSchema labelSchema) throws IOException {
        OutputWriter outputWriter = output.createOutputWriter(pathSupplier, kinesisConfig);
        return createPrinter(labelSchema, outputWriter);
    }

    public PropertyGraphPrinter createPrinterForNodes(String name, LabelSchema labelSchema) throws IOException {
        return createPrinterForNodes(() -> directories.createNodesFilePath(name, format, labelSchema.label(), perLabelDirectories), labelSchema);
    }

    private PropertyGraphPrinter createPrinterForNodes(Supplier<Path> pathSupplier, LabelSchema labelSchema) throws IOException {
        OutputWriter outputWriter = output.createOutputWriter(pathSupplier, kinesisConfig);
        return createPrinter(labelSchema, outputWriter);
    }

    public PropertyGraphTargetConfig forFileConsolidation() {
        return new PropertyGraphTargetConfig(directories, kinesisConfig, printerOptions, format, output, mergeFiles, perLabelDirectories, false);
    }

    private PropertyGraphPrinter createPrinter(LabelSchema labelSchema, OutputWriter outputWriter) throws IOException {
        if (inferSchema) {
            return format.createPrinterForInferredSchema(outputWriter, labelSchema, printerOptions);
        } else {
            return format.createPrinter(outputWriter, labelSchema, printerOptions);
        }
    }

    public RewriteCommand createRewriteCommand(ConcurrencyConfig concurrencyConfig, FeatureToggles featureToggles) {
        if (output.isFileBased()) {
            return format.createRewriteCommand(this, concurrencyConfig, inferSchema, featureToggles);
        } else {
            return masterLabelSchemas -> masterLabelSchemas;
        }
    }
}
