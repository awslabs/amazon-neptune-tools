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

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.cli.CloneClusterModule;
import com.amazonaws.services.neptune.cli.CommonConnectionModule;
import com.amazonaws.services.neptune.cli.RdfTargetModule;
import com.amazonaws.services.neptune.cluster.ClusterStrategy;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.rdf.NeptuneSparqlClient;
import com.amazonaws.services.neptune.rdf.io.ExportRdfGraphJob;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.help.Examples;

import javax.inject.Inject;
import java.nio.file.Path;

@Examples(examples = {
        "bin/neptune-export.sh export-rdf -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output "},

        descriptions = {
                "Export all data to the /home/ec2-user/output directory"
        })
@Command(name = "export-rdf", description = "Export RDF graph from Neptune to Turtle")
public class ExportRdfGraph extends NeptuneExportBaseCommand implements Runnable {

    @Inject
    private CloneClusterModule cloneStrategy = new CloneClusterModule();

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule();

    @Inject
    private RdfTargetModule target = new RdfTargetModule();

    @Override
    public void run() {

        try (Timer timer = new Timer("export-rdf");
             ClusterStrategy clusterStrategy = cloneStrategy.cloneCluster(connection.config(), new ConcurrencyConfig(1))) {

            Directories directories = target.createDirectories(DirectoryStructure.Rdf);

            try( NeptuneSparqlClient client = NeptuneSparqlClient.create(clusterStrategy.connectionConfig())) {

                ExportRdfGraphJob job = new ExportRdfGraphJob(client, target.config(directories));
                job.execute();
            }

            Path outputPath = directories.writeRootDirectoryPathAsReturnValue(target);
            onExportComplete(outputPath, new ExportStats());

        } catch (Exception e) {
            handleException(e);
        }
    }
}
