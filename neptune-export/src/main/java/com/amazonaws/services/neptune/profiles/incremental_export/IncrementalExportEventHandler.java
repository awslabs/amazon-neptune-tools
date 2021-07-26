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

package com.amazonaws.services.neptune.profiles.incremental_export;

import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.export.CompletionFileWriter;
import com.amazonaws.services.neptune.export.ExportToS3NeptuneExportEventHandler;
import com.amazonaws.services.neptune.export.NeptuneExportServiceEventHandler;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IncrementalExportEventHandler implements NeptuneExportServiceEventHandler, CompletionFileWriter {

    private final long timestamp;

    public IncrementalExportEventHandler() {
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public void updateCompletionFile(ObjectNode completionFilePayload) {

        ArrayNode partitions = JsonNodeFactory.instance.arrayNode();
        ObjectNode partition = JsonNodeFactory.instance.objectNode();
        partition.put("name", "timestamp");
        partition.put("value", String.valueOf(timestamp));
        partitions.add(partition);


        ObjectNode incrementalExportNode = JsonNodeFactory.instance.objectNode();
        completionFilePayload.set("incrementalExport", incrementalExportNode);
        incrementalExportNode.put("partitions", partitions);
    }

    @Override
    public void onBeforeExport(Args args, ExportToS3NeptuneExportEventHandler.S3UploadParams s3UploadParams) {

        if (args.contains("export-pg")) {


            if (args.contains("--format")) {
                args.removeOptions("--format");
            }


            if (args.contains("--partition-directories")) {
                args.removeOptions("--partition-directories");
            }

            args.addOption("--format", PropertyGraphExportFormat.neptuneStreamsSimpleJson.name());
            args.addOption("--partition-directories", String.format("timestamp=%s", timestamp));
        }

        s3UploadParams.setCreateExportSubdirectory(false).setOverwriteExisting(true);

    }

    @Override
    public void onError() {
        // Do nothing
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats) throws Exception {
        // Do nothing
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, GraphSchema graphSchema) throws Exception {
        // Do nothing
    }
}
