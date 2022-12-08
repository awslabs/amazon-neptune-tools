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

import com.amazonaws.services.neptune.cli.*;
import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.cluster.EventId;
import com.amazonaws.services.neptune.cluster.GetLastEventIdStrategy;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.rdf.NeptuneSparqlClient;
import com.amazonaws.services.neptune.rdf.ExportRdfJob;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.help.Examples;

import javax.inject.Inject;

@Examples(examples = {
        "bin/neptune-export.sh export-rdf -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output "},

        descriptions = {
                "Export all data to the /home/ec2-user/output directory"
        })
@Command(name = "export-rdf", description = "Export RDF graph from Neptune.")
public class ExportRdfGraph extends NeptuneExportCommand implements Runnable {

    @Inject
    private CloneClusterModule cloneStrategy = new CloneClusterModule();

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule(awsCli);

    @Inject
    private RdfTargetModule target = new RdfTargetModule();

    @Inject
    private RdfExportScopeModule exportScope = new RdfExportScopeModule();

    @Inject
    private NeptuneStreamsModule streams = new NeptuneStreamsModule();

    @Override
    public void run() {

        try {
            Timer.timedActivity(String.format("exporting rdf %s", exportScope.scope()), (CheckedActivity.Runnable) () -> {
                try (Cluster cluster = cloneStrategy.cloneCluster(
                        connection.clusterMetadata(),
                        connection.config(),
                        new ConcurrencyConfig(1),
                        featureToggles())) {

                    Directories directories = target.createDirectories();

                    JsonResource<EventId> eventIdFileResource = directories.lastEventIdFileResource();

                    GetLastEventIdStrategy getLastEventIdStrategy = streams.lastEventIdStrategy(cluster, eventIdFileResource);
                    getLastEventIdStrategy.saveLastEventId("sparql");

                    try (NeptuneSparqlClient client = NeptuneSparqlClient.create(cluster.connectionConfig())) {

                        ExportRdfJob job = exportScope.createJob(client, target.config(directories));
                        job.execute();
                    }

                    directories.writeRootDirectoryPathAsReturnValue(target);
                    getLastEventIdStrategy.writeLastEventIdResourcePathAsMessage(target);

                    onExportComplete(directories, new ExportStats(), cluster);

                }
            });
        } catch (Exception e) {
            handleException(e);
        }
    }
}
