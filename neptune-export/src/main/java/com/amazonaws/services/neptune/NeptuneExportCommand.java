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

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.cli.AwsCliModule;
import com.amazonaws.services.neptune.cli.FeatureToggleModule;
import com.amazonaws.services.neptune.cli.ProfilesModule;
import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.export.NeptuneExportEventHandler;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;

import javax.inject.Inject;

public abstract class NeptuneExportCommand extends NeptuneExportBaseCommand implements NeptuneExportEventHandler, NeptuneExportEventHandlerHost {

    @Inject
    protected AwsCliModule awsCli = new AwsCliModule();

    @Inject
    private FeatureToggleModule featureToggleModule = new FeatureToggleModule();

    @Inject
    private ProfilesModule profilesModule = new ProfilesModule();

    private boolean isCliInvocation = false;

    private NeptuneExportEventHandler eventHandler = NeptuneExportEventHandler.NULL_EVENT_HANDLER;

    @Override
    public void setEventHandler(NeptuneExportEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void setIsCliInvocation(boolean isCliInvocation) {
        this.isCliInvocation = isCliInvocation;
    }

    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster, GraphSchema graphSchema) throws Exception {
        eventHandler.onExportComplete(directories, stats, cluster, graphSchema);
    }

    public void onExportComplete(Directories directories, ExportStats stats, Cluster cluster) throws Exception {
        eventHandler.onExportComplete(directories, stats, cluster);
    }

    public void onError() {
        eventHandler.onError();
    }

    void handleException(Throwable e) {
        if (e.getCause() != null && RemoteConnectionException.class.isAssignableFrom(e.getCause().getClass())) {
            e.printStackTrace();
            System.err.println("An error occurred while connecting to Neptune. " +
                    "Ensure you have not disabled SSL if the database requires SSL in transit. " +
                    "Ensure you have specified the --use-iam-auth flag if the database uses IAM database authentication.");

        } else {
            e.printStackTrace();
            onError();
            System.err.println("An error occurred while exporting from Neptune: " + e.getMessage());
        }

        if (isCliInvocation) {
            System.exit(-1);
        }
    }

    FeatureToggles featureToggles() {
        return featureToggleModule.featureToggles();
    }
}
