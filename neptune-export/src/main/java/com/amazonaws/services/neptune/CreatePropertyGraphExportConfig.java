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

import com.amazonaws.services.neptune.cli.*;
import com.amazonaws.services.neptune.cluster.ClusterStrategy;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.propertygraph.metadata.*;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.neptune.io.Directories;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.help.Examples;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.inject.Inject;
import java.nio.file.Path;
import java.util.Collection;

@Examples(examples = {
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output",
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --sample --sample-size 100",
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS"
}, descriptions = {
        "Create metadata config file for all node and edge labels and save it to /home/ec2-user/output",
        "Create metadata config file for all node and edge labels, sampling 100 nodes and edges for each label",
        "Create config file containing metadata for User nodes and FOLLOWS edges"
})
@Command(name = "create-pg-config", description = "Create a property graph export metadata config file")
public class CreatePropertyGraphExportConfig extends NeptuneExportBaseCommand implements Runnable {

    @Inject
    private CloneClusterModule cloneStrategy = new CloneClusterModule();

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule();

    @Inject
    private PropertyGraphTargetModule target = new PropertyGraphTargetModule();

    @Inject
    private PropertyGraphScopeModule scope = new PropertyGraphScopeModule();

    @Inject
    private PropertyGraphConcurrencyModule concurrency = new PropertyGraphConcurrencyModule(false);

    @Inject
    private PropertyGraphSerializationModule serialization = new PropertyGraphSerializationModule();

    @Inject
    private PropertyGraphMetadataSamplingModule sampling = new PropertyGraphMetadataSamplingModule(() -> true);

    @Override
    public void run() {

        try (Timer timer = new Timer("create-pg-config");
             ClusterStrategy clusterStrategy = cloneStrategy.cloneCluster(connection.config(), concurrency.config())) {

            ExportStats stats = new ExportStats();
            Directories directories = target.createDirectories(DirectoryStructure.Config);
            JsonResource<PropertiesMetadataCollection> configFileResource = directories.configFileResource();
            Collection<ExportSpecification<?>> exportSpecifications = scope.exportSpecifications(stats, labModeFeatures());

            try (NeptuneGremlinClient client = NeptuneGremlinClient.create(clusterStrategy, serialization.config());
                 GraphTraversalSource g = client.newTraversalSource()) {

                MetadataCommand metadataCommand = sampling.createMetadataCommand(exportSpecifications, g);
                PropertiesMetadataCollection metadataCollection = metadataCommand.execute();

                configFileResource.save(metadataCollection);
                configFileResource.writeResourcePathAsMessage(target);
            }

            Path outputPath = directories.writeConfigFilePathAsReturnValue(target);
            onExportComplete(outputPath, stats);

        } catch (Exception e) {
            handleException(e);
        }
    }
}
