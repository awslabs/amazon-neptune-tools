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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.io.*;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphTargetConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class PropertyGraphTargetModule implements CommandWriter {

    @Option(name = {"-d", "--dir"}, description = "Root directory for output.")
    @Required
    @com.github.rvesse.airline.annotations.restrictions.Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File directory;

    @Option(name = {"-t", "--tag"}, description = "Directory prefix (optional).")
    @Once
    private String tag = "";

    @Option(name = {"--format"}, description = "Output format (optional, default 'csv').")
    @Once
    @AllowedEnumValues(PropertyGraphExportFormat.class)
    private PropertyGraphExportFormat format = PropertyGraphExportFormat.csv;

    @Option(name = {"-o", "--output"}, description = "Output target (optional, default 'file').")
    @Once
    @AllowedEnumValues(Target.class)
    private Target output = Target.files;

    @Option(name = {"--stream-name"}, description = "Name of an Amazon Kinesis Data Stream.")
    @Once
    private String streamName;

    @Option(name = {"--region"}, description = "AWS Region in which your Amazon Kinesis Data Stream is located.")
    @Once
    private String region;

    @Option(name = {"--merge-files"}, description = "Merge files for each vertex or edge label.")
    @Once
    private boolean mergeFiles = false;

    @Option(name = {"--export-id"}, description = "Export id", hidden = true)
    @Once
    private String exportId = UUID.randomUUID().toString().replace("-", "");

    private final boolean inferSchema;

    public PropertyGraphTargetModule(boolean inferSchema) {
        this.inferSchema = inferSchema;
    }

    public Directories createDirectories(DirectoryStructure directoryStructure) throws IOException {
        return Directories.createFor(directoryStructure, directory, exportId, tag );
    }

    public PropertyGraphTargetConfig config(Directories directories, PrinterOptions printerOptions){
        KinesisConfig kinesisConfig = new KinesisConfig(streamName, region);
        return new PropertyGraphTargetConfig(directories, kinesisConfig, printerOptions, format, output, inferSchema, mergeFiles);
    }

    public String description(){
        return format.description();
    }

    @Override
    public void writeReturnValue(String value){
        output.writeReturnValue(value);
    }

    @Override
    public void writeMessage(String value) {
        output.writeMessage(value);
    }
}
