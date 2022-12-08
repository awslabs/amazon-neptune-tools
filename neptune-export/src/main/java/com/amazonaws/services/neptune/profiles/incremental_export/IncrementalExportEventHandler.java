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

import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.cluster.EventId;
import com.amazonaws.services.neptune.cluster.GetLastEventId;
import com.amazonaws.services.neptune.cluster.NeptuneClusterMetadata;
import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.export.CompletionFileWriter;
import com.amazonaws.services.neptune.export.ExportToS3NeptuneExportEventHandler;
import com.amazonaws.services.neptune.export.NeptuneExportServiceEventHandler;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.rdf.io.RdfExportFormat;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class IncrementalExportEventHandler implements NeptuneExportServiceEventHandler, CompletionFileWriter {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(IncrementalExportEventHandler.class);

    private final long timestamp;
    private final AtomicLong commitNum = new AtomicLong(0);
    private final AtomicLong opNum = new AtomicLong(0);
    private final String exportId;
    private final String stageId;
    private final String command;

    public IncrementalExportEventHandler(ObjectNode additionalParams) {
        this.timestamp = System.currentTimeMillis();

        JsonNode incrementalExport = additionalParams.path("incremental_export");

        this.exportId = incrementalExport.path("exportId").textValue();;
        this.stageId = incrementalExport.path("stageId").textValue();
        this.command = incrementalExport.path("command").textValue();

        logger.info("Incremental export params: exportId: {}, stageId: {}, command: {}", exportId, stageId, command);
    }

    @Override
    public void updateCompletionFile(ObjectNode completionFilePayload) {

        ArrayNode partitions = JsonNodeFactory.instance.arrayNode();
        ObjectNode partition = JsonNodeFactory.instance.objectNode();
        partition.put("name", "timestamp");
        partition.put("value", String.valueOf(timestamp));
        partitions.add(partition);

        ObjectNode lastEventId = JsonNodeFactory.instance.objectNode();
        lastEventId.put("commitNum", commitNum.get());
        lastEventId.put("opNum", opNum.get());

        ObjectNode incrementalExportNode = JsonNodeFactory.instance.objectNode();
        completionFilePayload.set("incrementalExport", incrementalExportNode);
        incrementalExportNode.put("exportId", exportId);
        incrementalExportNode.put("stageId", stageId);
        incrementalExportNode.set("partitions", partitions);
        incrementalExportNode.set("lastEventId", lastEventId);
    }

    @Override
    public void onBeforeExport(Args args, ExportToS3NeptuneExportEventHandler.S3UploadParams s3UploadParams) {

        if (args.contains("--format")) {
            args.removeOptions("--format");
        }

        if (args.contains("--partition-directories")) {
            args.removeOptions("--partition-directories");
        }

        boolean createExportSubdirectory = true;

        if (command.equals("apply")){
            args.addOption("--partition-directories", String.format("timestamp=%s", timestamp));
            createExportSubdirectory = false;
            if (args.contains("export-pg")) {
                args.addOption("--format", PropertyGraphExportFormat.neptuneStreamsSimpleJson.name());
            } else {
                args.addOption("--format", RdfExportFormat.neptuneStreamsSimpleJson.name());
            }
        } else {
            if (args.contains("export-pg")) {
                args.addOption("--format", PropertyGraphExportFormat.csv.name());
            } else {
                args.addOption("--format", RdfExportFormat.nquads.name());
            }
        }

        s3UploadParams.setCreateExportSubdirectory(createExportSubdirectory).setOverwriteExisting(true);
    }

    @Override
    public void onError() {
        // Do nothing
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster) throws Exception {
        onExportComplete(directories, stats, cluster, null);
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster, GraphSchema graphSchema) throws Exception {
        Timer.timedActivity("getting LastEventId from stream", (CheckedActivity.Runnable) () ->
                getLastEventIdFromStream(cluster, graphSchema == null ? "sparql" : "gremlin"));
    }

    private void getLastEventIdFromStream(Cluster cluster, String streamEndpointType) {

        EventId eventId = new GetLastEventId(cluster.clusterMetadata(), cluster.connectionConfig(), streamEndpointType).execute();
        if (eventId != null) {
            commitNum.set(eventId.commitNum());
            opNum.set(eventId.opNum());
        }
    }
}
