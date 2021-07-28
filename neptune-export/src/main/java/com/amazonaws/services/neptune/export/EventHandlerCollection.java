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

package com.amazonaws.services.neptune.export;

import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class EventHandlerCollection implements NeptuneExportServiceEventHandler {

    private final List<NeptuneExportEventHandler> exportHandlers = new ArrayList<>();
    private final List<NeptuneExportServiceEventHandler> serviceHandlers = new ArrayList<>();

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EventHandlerCollection.class);

    public <T extends NeptuneExportEventHandler> void addHandler(T handler){
        exportHandlers.add(handler);
        if (NeptuneExportServiceEventHandler.class.isAssignableFrom(handler.getClass())){
            serviceHandlers.add((NeptuneExportServiceEventHandler) handler);
        }
    }

    @Override
    public void onError() {
        for (NeptuneExportEventHandler handler : exportHandlers) {
            try {
                handler.onError();
            } catch (Exception e) {
                logger.warn("Error while handling export error with {}", handler.getClass().getSimpleName(), e);
            }
        }
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster) throws Exception {
        boolean error = false;

        for (int i = exportHandlers.size(); i-- > 0; ) {
            NeptuneExportEventHandler handler = exportHandlers.get(i);
            try {
                handler.onExportComplete(directories, stats, cluster);
            } catch (Exception e) {
                error = true;
                logger.error("Error while executing {}", handler.getClass().getSimpleName(), e);
            }
        }

        if (error){
            throw new RuntimeException("One or more errors occurred while executing onExportComplete event handlers. See the logs for details.");
        }
    }

    @Override
    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster, GraphSchema graphSchema) throws Exception {
        boolean error = false;

        for (int i = exportHandlers.size(); i-- > 0; ) {
            NeptuneExportEventHandler handler = exportHandlers.get(i);
            try {
                handler.onExportComplete(directories, stats, cluster, graphSchema);
            } catch (Exception e) {
                error = true;
                logger.error("Error while executing {}", handler.getClass().getSimpleName(), e);
            }
        }

        if (error){
            throw new RuntimeException("One or more errors occurred while executing onExportComplete event handlers. See the logs for details.");
        }
    }

    @Override
    public void onBeforeExport(Args args, ExportToS3NeptuneExportEventHandler.S3UploadParams s3UploadParams) {
        boolean error = false;

        for (NeptuneExportServiceEventHandler handler : serviceHandlers) {
            try {
                handler.onBeforeExport(args, s3UploadParams);
            } catch (Exception e) {
                error = true;
                logger.error("Error while executing {}", handler.getClass().getSimpleName(), e);
            }
        }

        if (error){
            throw new RuntimeException("One or more errors occurred while executing onBeforeExport event handlers. See the logs for details.");
        }
    }
}
