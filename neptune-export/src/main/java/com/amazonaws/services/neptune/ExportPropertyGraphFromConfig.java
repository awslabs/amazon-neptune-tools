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
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.io.ExportPropertyGraphJob;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphTargetConfig;
import com.amazonaws.services.neptune.propertygraph.metadata.CreateMetadataFromConfigFile;
import com.amazonaws.services.neptune.propertygraph.metadata.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadataCollection;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.inject.Inject;
import java.io.File;
import java.util.Collection;

@Examples(examples = {
        "bin/neptune-export.sh export-pg-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output",
        "bin/neptune-export.sh export-pg-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output --format json"
}, descriptions = {
        "Export data using the metadata config in /home/ec2-user/config.json",
        "Export data as JSON using the metadata config in /home/ec2-user/config.json"
})
@Command(name = "export-pg-from-config", description = "Export property graph from Neptune to CSV or JSON using an existing config file")
public class ExportPropertyGraphFromConfig extends NeptuneExportBaseCommand implements Runnable {

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule();

    @Inject
    private CommonFileSystemModule fileSystem = new CommonFileSystemModule();

    @Inject
    private PropertyGraphScopeModule scope = new PropertyGraphScopeModule();

    @Inject
    private PropertyGraphTargetModule target = new PropertyGraphTargetModule();

    @Inject
    private PropertyGraphConcurrencyModule concurrency = new PropertyGraphConcurrencyModule();

    @Inject
    private PropertyGraphSerializationModule serialization = new PropertyGraphSerializationModule();

    @Inject
    private PropertyGraphRangeModule range = new PropertyGraphRangeModule();

    @Option(name = {"-c", "--config-file"}, description = "Path to JSON config file")
    @Required
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File configFile;

    @Option(name = {"--exclude-type-definitions"}, description = "Exclude type definitions from column headers (optional, default 'false')")
    @Once
    private boolean excludeTypeDefinitions = false;

    @Override
    public void run() {

        try (Timer timer = new Timer();
             NeptuneGremlinClient client = NeptuneGremlinClient.create(connection.config(), concurrency.config(), serialization.config());
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = fileSystem.createDirectories(DirectoryStructure.PropertyGraph);
            PropertyGraphTargetConfig targetConfig = target.config(directories, !excludeTypeDefinitions);

            PropertiesMetadataCollection metadataCollection = new CreateMetadataFromConfigFile(configFile).execute();

            ExportStats stats = new ExportStats();
            stats.prepare(metadataCollection);

            Collection<ExportSpecification<?>> exportSpecifications = scope.exportSpecifications(stats);

            ExportPropertyGraphJob exportJob = new ExportPropertyGraphJob(
                    exportSpecifications,
                    metadataCollection,
                    g,
                    range.config(),
                    concurrency.config(),
                    targetConfig);
            exportJob.execute();

            System.err.println();
            System.err.println(target.description() + " files : " + directories.directory());
            System.err.println("Config file : " + configFile.getAbsolutePath());
            System.err.println();
            System.err.println(stats.toString());

            target.writeCommandResult(directories.directory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }
}
