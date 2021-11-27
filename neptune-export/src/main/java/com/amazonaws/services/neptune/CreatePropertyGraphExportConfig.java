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
import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.io.Target;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.io.ExportPropertyGraphJob;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphTargetConfig;
import com.amazonaws.services.neptune.propertygraph.schema.CreateGraphSchemaCommand;
import com.amazonaws.services.neptune.propertygraph.schema.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.help.Examples;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.inject.Inject;
import java.util.Collection;

@Examples(examples = {
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output",
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --sample --sample-size 100",
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS"
}, descriptions = {
        "Create schema config file for all node and edge labels and save it to /home/ec2-user/output",
        "Create schema config file for all node and edge labels, sampling 100 nodes and edges for each label",
        "Create config file containing schema for User nodes and FOLLOWS edges"
})
@Command(name = "create-pg-config", description = "Create a property graph schema config file.")
public class CreatePropertyGraphExportConfig extends NeptuneExportCommand implements Runnable {

    @Inject
    private CloneClusterModule cloneStrategy = new CloneClusterModule(awsCli);

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule(awsCli);

    @Inject
    private PropertyGraphTargetModule target = new PropertyGraphTargetModule(Target.devnull);

    @Inject
    private PropertyGraphScopeModule scope = new PropertyGraphScopeModule();

    @Inject
    private PropertyGraphConcurrencyModule concurrency = new PropertyGraphConcurrencyModule();

    @Inject
    private PropertyGraphSerializationModule serialization = new PropertyGraphSerializationModule();

    @Inject
    private PropertyGraphSchemaInferencingModule sampling = new PropertyGraphSchemaInferencingModule();

    @Inject
    private GremlinFiltersModule gremlinFilters = new GremlinFiltersModule();

    @Override
    public void run() {

        try {
            Timer.timedActivity("creating property graph config", (CheckedActivity.Runnable) () -> {
                try (Cluster cluster = cloneStrategy.cloneCluster(connection.config(), concurrency.config(sampling.isFullScan()), featureToggles())) {

                    if (sampling.isFullScan()) {

                        Directories directories = target.createDirectories(DirectoryStructure.Config);
                        JsonResource<GraphSchema> configFileResource = directories.configFileResource();

                        GraphSchema graphSchema = new GraphSchema();
                        ExportStats stats = new ExportStats();

                        PropertyGraphTargetConfig targetConfig = target.config(directories, new PrinterOptionsModule().config());

                        Collection<ExportSpecification> exportSpecifications = scope.exportSpecifications(
                                graphSchema,
                                gremlinFilters.filters(),
                                stats,
                                featureToggles());

                        try (NeptuneGremlinClient client = NeptuneGremlinClient.create(cluster, serialization.config());
                             GraphTraversalSource g = client.newTraversalSource()) {

                            ExportPropertyGraphJob exportJob = new ExportPropertyGraphJob(
                                    exportSpecifications,
                                    graphSchema,
                                    g,
                                    new PropertyGraphRangeModule().config(),
                                    gremlinFilters.filters(),
                                    cluster.concurrencyConfig(),
                                    targetConfig, featureToggles());

                            graphSchema = exportJob.execute();

                            configFileResource.save(graphSchema);
                        }

                        directories.writeRootDirectoryPathAsMessage(target.description(), target);
                        configFileResource.writeResourcePathAsMessage(target);

                        System.err.println();
                        System.err.println(stats.formatStats(graphSchema));

                        directories.writeRootDirectoryPathAsReturnValue(target);
                        onExportComplete(directories, stats, cluster, graphSchema);

                    } else {

                        ExportStats stats = new ExportStats();
                        Directories directories = target.createDirectories(DirectoryStructure.Config);
                        JsonResource<GraphSchema> configFileResource = directories.configFileResource();
                        Collection<ExportSpecification> exportSpecifications = scope.exportSpecifications(
                                stats,
                                gremlinFilters.filters(),
                                featureToggles());

                        try (NeptuneGremlinClient client = NeptuneGremlinClient.create(cluster, serialization.config());
                             GraphTraversalSource g = client.newTraversalSource()) {

                            CreateGraphSchemaCommand createGraphSchemaCommand = sampling.createSchemaCommand(exportSpecifications, g);
                            GraphSchema graphSchema = createGraphSchemaCommand.execute();

                            configFileResource.save(graphSchema);
                            configFileResource.writeResourcePathAsMessage(target);
                        }

                        directories.writeConfigFilePathAsReturnValue(target);
                        onExportComplete(directories, stats, cluster);
                    }
                }
            });
        } catch (Exception e) {
            handleException(e);
        }
    }
}
