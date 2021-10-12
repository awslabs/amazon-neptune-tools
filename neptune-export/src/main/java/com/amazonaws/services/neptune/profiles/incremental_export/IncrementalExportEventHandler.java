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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.neptune.cluster.*;
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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class IncrementalExportEventHandler implements NeptuneExportServiceEventHandler, CompletionFileWriter {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(IncrementalExportEventHandler.class);

    private final long timestamp;
    private final AtomicLong commitNum = new AtomicLong(0);
    private final AtomicLong opNum = new AtomicLong(0);
    private final String exportId;

    public IncrementalExportEventHandler(String exportId) {
        this.exportId = exportId;
        this.timestamp = System.currentTimeMillis();
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

        args.addOption("--partition-directories", String.format("timestamp=%s", timestamp));

        if (args.contains("export-pg")) {
            args.addOption("--format", PropertyGraphExportFormat.neptuneStreamsSimpleJson.name());
        } else {
            args.addOption("--format", RdfExportFormat.neptuneStreamsSimpleJson.name());
        }

        s3UploadParams.setCreateExportSubdirectory(false).setOverwriteExisting(true);

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
                getLastEventIdFromStream(cluster,  graphSchema == null ? "sparql" : "gremlin"));
    }

    private void getLastEventIdFromStream(Cluster cluster, String streamEndpointType) {

        NeptuneClusterMetadata clusterMetadata = NeptuneClusterMetadata.createFromClusterId(cluster.connectionConfig().clusterId(), cluster.clientSupplier());

        EventId eventId = new GetLastEventId(clusterMetadata, cluster.connectionConfig(), streamEndpointType).execute();
        if (eventId != null){
            commitNum.set(eventId.commitNum());
            opNum.set(eventId.opNum());
        }
    }
}
