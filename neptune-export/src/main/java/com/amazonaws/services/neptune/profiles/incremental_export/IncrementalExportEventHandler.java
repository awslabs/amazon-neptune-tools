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
import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.cluster.NeptuneClusterMetadata;
import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.export.CompletionFileWriter;
import com.amazonaws.services.neptune.export.ExportToS3NeptuneExportEventHandler;
import com.amazonaws.services.neptune.export.NeptuneExportServiceEventHandler;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
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

        ObjectNode lastEventId = JsonNodeFactory.instance.objectNode();
        lastEventId.put("commitNum", commitNum.get());
        lastEventId.put("opNum", opNum.get());

        ObjectNode incrementalExportNode = JsonNodeFactory.instance.objectNode();
        completionFilePayload.set("incrementalExport", incrementalExportNode);
        incrementalExportNode.set("partitions", partitions);
        incrementalExportNode.set("lastEventId", lastEventId);
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
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster) throws Exception {
        // Do nothing
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster, GraphSchema graphSchema) throws Exception {
        NeptuneClusterMetadata clusterMetadata = NeptuneClusterMetadata.createFromClusterId(cluster.connectionConfig().clusterId(), cluster.clientSupplier());

        if (clusterMetadata.isStreamEnabled()) {
            Timer.timedActivity("getting LastEventId from stream", (CheckedActivity.Runnable) () -> getLastEventIdFromStream(clusterMetadata));
        }
    }

    private void getLastEventIdFromStream(NeptuneClusterMetadata clusterMetadata) {
        String streamsEndpoint = String.format("https://%s:%s/gremlin/stream", clusterMetadata.endpoints().get(0), clusterMetadata.port());
        logger.info("Streams endpoint: {}", streamsEndpoint);

        try {

            String region = new DefaultAwsRegionProviderChain().getRegion();
            NeptuneHttpsClient neptuneHttpsClient = new NeptuneHttpsClient(streamsEndpoint, region);

            Map<String, String> params = new HashMap<>();
            params.put("commitNum", String.valueOf(Long.MAX_VALUE));
            params.put("limit", "1");

            HttpResponse httpResponse = neptuneHttpsClient.get(params);

            logger.info(httpResponse.getContent());

        } catch (AmazonServiceException e) {

            if (e.getErrorCode().equals("StreamRecordsNotFoundException")){
                StreamRecordsNotFoundExceptionParser.LastEventId lastEventId = StreamRecordsNotFoundExceptionParser.parseLastEventId(e.getErrorMessage());
                commitNum.set(lastEventId.commitNum());
                opNum.set(lastEventId.opNum());
                logger.info("LastEventId: {}", lastEventId);
            } else {
                logger.error("Error while accessing Neptune Streams endpoint", e);
            }

        } catch (Exception e){
            logger.error("Error while accessing Neptune Streams endpoint", e);
        }
    }
}
