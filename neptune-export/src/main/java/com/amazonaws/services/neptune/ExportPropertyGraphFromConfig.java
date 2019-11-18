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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.propertygraph.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.io.ExportPropertyGraphJob;
import com.amazonaws.services.neptune.propertygraph.io.KinesisConfig;
import com.amazonaws.services.neptune.propertygraph.io.TargetConfig;
import com.amazonaws.services.neptune.propertygraph.metadata.CreateMetadataFromConfigFile;
import com.amazonaws.services.neptune.propertygraph.metadata.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadataCollection;
import com.amazonaws.services.neptune.propertygraph.metadata.TokensOnly;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

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
public class ExportPropertyGraphFromConfig extends NeptuneExportPropertyGraphBaseCommand implements Runnable {

    @Option(name = {"-c", "--config-file"}, description = "Path to JSON config file")
    @Required
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File configFile;

    @Override
    public void run() {
        ConcurrencyConfig concurrencyConfig = new ConcurrencyConfig(concurrency, rangeSize, skip, limit);

        try (Timer timer = new Timer();
             NeptuneGremlinClient client = NeptuneGremlinClient.create(connectionConfig(), concurrencyConfig);
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = Directories.createFor(DirectoryStructure.PropertyGraph, directory, tag);
            KinesisConfig kinesisConfig = new KinesisConfig(streamName, region);
            TargetConfig targetConfig = new TargetConfig(directories, format, output, !excludeTypeDefinitions, kinesisConfig);

            PropertiesMetadataCollection metadataCollection = new CreateMetadataFromConfigFile(configFile).execute();

            ExportStats stats = new ExportStats();
            stats.prepare(metadataCollection);

            Collection<ExportSpecification<?>> exportSpecifications = scope.exportSpecifications(nodeLabels, edgeLabels, TokensOnly.off, stats);

            ExportPropertyGraphJob exportJob = new ExportPropertyGraphJob(
                    exportSpecifications,
                    metadataCollection,
                    g,
                    concurrencyConfig,
                    targetConfig
            );
            exportJob.execute();

            System.err.println();
            System.err.println(format.description() + " files   : " + directories.directory());
            System.err.println("Config file : " + configFile.getAbsolutePath());
            System.err.println();
            System.err.println(stats.toString());

            output.writeCommandResult(directories.directory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }
}
