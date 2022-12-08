/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

    @Option(name = {"--region", "--stream-region"}, description = "AWS Region in which your Amazon Kinesis Data Stream is located.")
    @Once
    private String region;

    @Option(name = {"--stream-large-record-strategy"}, description = "Strategy for dealing with records to be sent to Amazon Kinesis that are larger than 1 MB.")
    @Once
    @AllowedEnumValues(LargeStreamRecordHandlingStrategy.class)
    private LargeStreamRecordHandlingStrategy largeStreamRecordHandlingStrategy = LargeStreamRecordHandlingStrategy.splitAndShred;

    @Option(name = {"--merge-files"}, description = "Merge files for each vertex or edge label (currently only supports CSV files for export-pg).")
    @Once
    private boolean mergeFiles = false;

    @Option(name = {"--export-id"}, description = "Export id")
    @Once
    private String exportId = UUID.randomUUID().toString().replace("-", "");

    @Option(name = {"--per-label-directories"}, description = "Create a subdirectory for each distinct vertex or edge label.")
    @Once
    private boolean perLabelDirectories = false;

    @Option(name = {"--partition-directories"}, description = "Partition directory path (e.g. 'year=2021/month=07/day=21').")
    @Once
    private String partitionDirectories = "";

    public PropertyGraphTargetModule() {
    }

    public PropertyGraphTargetModule(Target target) {
        this.output =  target;
    }

    public Directories createDirectories() throws IOException {
        return Directories.createFor(directoryStructure(), directory, exportId, tag, partitionDirectories );
    }

    public Directories createDirectories(DirectoryStructure directoryStructure) throws IOException {
        return Directories.createFor(directoryStructure, directory, exportId, tag, partitionDirectories );
    }

    public PropertyGraphTargetConfig config(Directories directories, PrinterOptions printerOptions){

        if (mergeFiles && (format != PropertyGraphExportFormat.csv && format != PropertyGraphExportFormat.csvNoHeaders)){
            throw new IllegalArgumentException("Merge files is only supported for CSV formats for export-pg");
        }

        KinesisConfig kinesisConfig = new KinesisConfig(streamName, region, largeStreamRecordHandlingStrategy);
        
        return new PropertyGraphTargetConfig(directories, kinesisConfig, printerOptions, format, output, mergeFiles, perLabelDirectories, true);
    }

    public String description(){
        return format.description();
    }

    private DirectoryStructure directoryStructure(){
        if (format == PropertyGraphExportFormat.neptuneStreamsSimpleJson){
            return DirectoryStructure.SimpleStreamsOutput;
        } else {
            return DirectoryStructure.PropertyGraph;
        }
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
