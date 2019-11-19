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
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.metadata.*;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.neptune.io.Directories;
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
        "Create metadata config file for all node and edge labels and save it to /home/ec2-user/output",
        "Create metadata config file for all node and edge labels, sampling 100 nodes and edges for each label",
        "Create config file containing metadata for User nodes and FOLLOWS edges"
})
@Command(name = "create-pg-config", description = "Create a property graph export metadata config file")
public class CreatePropertyGraphExportConfig extends NeptuneExportBaseCommand implements Runnable {

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule();

    @Inject
    private CommonFileSystemModule fileSystem = new CommonFileSystemModule();

    @Inject
    private PropertyGraphScopeModule scope = new PropertyGraphScopeModule();

    @Inject
    private PropertyGraphConcurrencyModule concurrency = new PropertyGraphConcurrencyModule(false);

    @Inject
    private PropertyGraphSerializationModule serialization = new PropertyGraphSerializationModule();

    @Inject
    private PropertyGraphMetadataSamplingModule sampling = new PropertyGraphMetadataSamplingModule();

    @Override
    public void run() {

        try (Timer timer = new Timer();
             NeptuneGremlinClient client = NeptuneGremlinClient.create(connection.config(), concurrency.config(), serialization.config());
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = fileSystem.createDirectories(DirectoryStructure.Config);
            java.nio.file.Path configFilePath = directories.configFilePath();

            ExportStats stats = new ExportStats();
            Collection<ExportSpecification<?>> exportSpecifications = scope.exportSpecifications(stats);

            MetadataCommand metadataCommand = sampling.createMetadataCommand(exportSpecifications, g);
            PropertiesMetadataCollection metadataCollection = metadataCommand.execute();

            new SaveMetadataConfig(metadataCollection, configFilePath).execute();

            System.err.println("Config file : " + configFilePath);
            System.out.println(configFilePath);

        } catch (Exception e) {
            System.err.println("An error occurred while creating export config:");
            e.printStackTrace();
        }
    }
}
