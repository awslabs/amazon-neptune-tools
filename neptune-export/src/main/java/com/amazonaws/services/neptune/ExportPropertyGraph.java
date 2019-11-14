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
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.io.ExportPropertyGraphJob;
import com.amazonaws.services.neptune.propertygraph.io.Format;
import com.amazonaws.services.neptune.propertygraph.io.Output;
import com.amazonaws.services.neptune.propertygraph.io.TargetConfig;
import com.amazonaws.services.neptune.propertygraph.metadata.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadataCollection;
import com.amazonaws.services.neptune.propertygraph.metadata.SaveMetadataConfig;
import com.amazonaws.services.neptune.propertygraph.metadata.TokensOnly;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Examples(examples = {
        "bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output",
        "bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --format json",
        "bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -s nodes",
        "bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS",
        "bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -cn 2",
        "bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -cn 2 -r 1000"
}, descriptions = {
        "Export all data to the /home/ec2-user/output directory",
        "Export all data to the /home/ec2-user/output directory as JSON",
        "Export only nodes to the /home/ec2-user/output directory",
        "Export only User nodes and FOLLOWS relationships",
        "Parallel export using 2 threads",
        "Parallel export using 2 threads, with each thread processing batches of 1000 nodes or edges"
})
@Command(name = "export-pg", description = "Export property graph from Neptune to CSV or JSON")
public class ExportPropertyGraph extends NeptuneExportBaseCommand implements Runnable {

    @Option(name = {"-nl", "--node-label"}, description = "Labels of nodes to be exported (optional, default all labels)",
            arity = 1)
    private List<String> nodeLabels = new ArrayList<>();

    @Option(name = {"-el", "--edge-label"}, description = "Labels of edges to be exported (optional, default all labels)",
            arity = 1)
    private List<String> edgeLabels = new ArrayList<>();

    @Option(name = {"-r", "--range", "--range-size"}, description = "Number of items to fetch per request (optional)")
    @Once
    private long rangeSize = -1;

    @Option(name = {"--limit"}, description = "Maximum number of items to export (optional)")
    @Once
    private long limit = Long.MAX_VALUE;

    @Option(name = {"--skip"}, description = "Number of items to skip (optional)")
    @Once
    private long skip = 0;

    @Option(name = {"-cn", "--concurrency"}, description = "Concurrency (optional)")
    @Once
    private int concurrency = 1;

    @Option(name = {"-s", "--scope"}, description = "Scope (optional, default 'all')")
    @Once
    @AllowedValues(allowedValues = {"all", "nodes", "edges"})
    private Scope scope = Scope.all;

    @Option(name = {"--sample"}, description = "Select only a subset of nodes and edges when generating property metadata")
    @Once
    private boolean sample = false;

    @Option(name = {"--sample-size"}, description = "Property metadata sample size (optional, default 1000")
    @Once
    private long sampleSize = 1000;

    @Option(name = {"--format"}, description = "Output format (optional, default 'csv')")
    @Once
    @AllowedValues(allowedValues = {"csv", "csvNoHeaders", "json", "neptuneStreamsJson"})
    private Format format = Format.csv;

    @Option(name = {"-o", "--output"}, description = "Output target (optional, default 'file')")
    @Once
    @AllowedValues(allowedValues = {"files", "stdout"})
    private Output output = Output.files;

    @Option(name = {"--exclude-type-definitions"}, description = "Exclude type definitions from column headers (optional, default 'false')")
    @Once
    private boolean excludeTypeDefinitions = false;

    @Option(name = {"--tokens-only"}, description = "Export tokens (~id, ~label) only (optional, default 'off')")
    @Once
    @AllowedValues(allowedValues = {"off", "nodes", "edges", "both"})
    private TokensOnly tokensOnly = TokensOnly.off;

    @Override
    public void run() {
        ConcurrencyConfig concurrencyConfig = new ConcurrencyConfig(concurrency, rangeSize, skip, limit);
        MetadataSamplingSpecification metadataSamplingSpecification = new MetadataSamplingSpecification(sample, sampleSize);

        try (Timer timer = new Timer();
             NeptuneGremlinClient client = NeptuneGremlinClient.create(connectionConfig(), concurrencyConfig);
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = Directories.createFor(DirectoryStructure.PropertyGraph, directory, tag);
            java.nio.file.Path configFilePath = directories.configFilePath().toAbsolutePath();

            TargetConfig targetConfig = new TargetConfig(directories, format, output, !excludeTypeDefinitions);

            ExportStats stats = new ExportStats();

            Collection<ExportSpecification<?>> exportSpecifications =
                    scope.exportSpecifications(nodeLabels, edgeLabels, tokensOnly, stats);

            PropertiesMetadataCollection metadataCollection =
                    metadataSamplingSpecification.createMetadataCommand(exportSpecifications, g).execute();

            stats.prepare(metadataCollection);

            new SaveMetadataConfig(metadataCollection, configFilePath).execute();

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
            System.err.println("Config file : " + configFilePath);
            System.err.println();
            System.err.println(stats.toString());

            output.writeCommandResult(directories.directory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }
}
