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
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;
import com.amazonaws.services.neptune.rdf.io.RdfExportFormat;
import com.amazonaws.services.neptune.rdf.io.RdfTargetConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class RdfTargetModule implements CommandWriter {

    @Option(name = {"-d", "--dir"}, description = "Root directory for output.")
    @Required
    @com.github.rvesse.airline.annotations.restrictions.Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File directory;

    @Option(name = {"-t", "--tag"}, description = "Directory prefix (optional).")
    @Once
    private String tag = "";

    @Option(name = {"-o", "--output"}, description = "Output target (optional, default 'file').")
    @Once
    @AllowedEnumValues(Target.class)
    private Target output = Target.files;

    @Option(name = {"--format"}, description = "Output format (optional, default 'turtle').")
    @Once
    @AllowedEnumValues(RdfExportFormat.class)
    private RdfExportFormat format = RdfExportFormat.turtle;

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

    @Option(name = {"--export-id"}, description = "Export ID")
    @Once
    private String exportId = UUID.randomUUID().toString().replace("-", "");

    @Option(name = {"--partition-directories"}, description = "Partition directory path (e.g. 'year=2021/month=07/day=21').")
    @Once
    private String partitionDirectories = "";

    public Directories createDirectories() throws IOException {
        return Directories.createFor(directoryStructure(), directory, exportId, tag, partitionDirectories );
    }

    public Directories createDirectories(DirectoryStructure directoryStructure) throws IOException {
        return Directories.createFor(directoryStructure, directory, exportId, tag, partitionDirectories );
    }

    public RdfTargetConfig config(Directories directories) {
        return new RdfTargetConfig(directories, new KinesisConfig(streamName, region, largeStreamRecordHandlingStrategy), output, format);
    }

    @Override
    public void writeReturnValue(String value){
        output.writeReturnValue(value);
    }

    @Override
    public void writeMessage(String value) {
        output.writeMessage(value);
    }

    private DirectoryStructure directoryStructure(){
        if (format == RdfExportFormat.neptuneStreamsSimpleJson){
            return DirectoryStructure.SimpleStreamsOutput;
        } else {
            return DirectoryStructure.Rdf;
        }
    }
}
