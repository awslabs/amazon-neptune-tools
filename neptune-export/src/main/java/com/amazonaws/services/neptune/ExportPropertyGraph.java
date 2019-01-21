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

import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.io.ExportJob;
import com.amazonaws.services.neptune.io.Format;
import com.amazonaws.services.neptune.metadata.MetadataCommand;
import com.amazonaws.services.neptune.metadata.MetadataSpecification;
import com.amazonaws.services.neptune.metadata.PropertiesMetadataCollection;
import com.amazonaws.services.neptune.metadata.SaveMetadataConfig;
import com.amazonaws.services.neptune.util.Timer;
import com.amazonaws.services.neptune.io.Directories;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.File;
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
public class ExportPropertyGraph implements Runnable {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint(s) – supply multiple instance endpoints if you want to load balance requests across a cluster")
    @Required
    private List<String> endpoints;

    @Option(name = {"-p", "--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.USER})
    @Once
    private int port = 8182;

    @Option(name = {"-d", "--dir"}, description = "Root directory for output")
    @Required
    @Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File directory;

    @Option(name = {"-t", "--tag"}, description = "Directory prefix (optional)")
    @Once
    private String tag = "";

    @Option(name = {"-nl", "--node-label"}, description = "Labels of nodes to be exported (optional, default all labels)",
            arity = 1)
    private List<String> nodeLabels = new ArrayList<>();

    @Option(name = {"-el", "--edge-label"}, description = "Labels of edges to be exported (optional, default all labels)",
            arity = 1)
    private List<String> edgeLabels = new ArrayList<>();

    @Option(name = {"-r", "--range"}, description = "Range (optional)")
    @Once
    private long range = -1;

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

    @Option(name = {"--use-iam-auth"}, description = "Use IAM database authentication to authenticate to Neptune")
    @Once
    private boolean useIamAuth = false;

    @Option(name = {"--sample-size"}, description = "Property metadata sample size (optional, default 1000")
    @Once
    private long sampleSize = 1000;

    @Option(name = {"--format"}, description = "Output format (optional, default 'csv')")
    @Once
    @AllowedValues(allowedValues = {"csv", "json"})
    private Format format = Format.csv;

    @Override
    public void run() {
        ConcurrencyConfig concurrencyConfig = new ConcurrencyConfig(concurrency, range);
        MetadataSamplingSpecification metadataSamplingSpecification = new MetadataSamplingSpecification(sample, sampleSize);

        try (Timer timer = new Timer();
             NeptuneGremlinClient client = NeptuneGremlinClient.create(endpoints, port, concurrencyConfig, useIamAuth);
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = Directories.createFor(directory, tag);
            java.nio.file.Path configFilePath = directories.configFilePath().toAbsolutePath();

            Collection<MetadataSpecification<?>> metadataSpecifications = scope.metadataSpecifications(nodeLabels, edgeLabels);

            MetadataCommand metadataCommand = metadataSamplingSpecification.createMetadataCommand(metadataSpecifications, g);
            PropertiesMetadataCollection metadataCollection = metadataCommand.execute();

            new SaveMetadataConfig(metadataCollection, configFilePath).execute();

            ExportJob exportJob = new ExportJob(metadataSpecifications, metadataCollection, g, concurrencyConfig, directories, format);
            exportJob.execute();

            System.err.println(format.description() + " files   : " + directories.directory());
            System.err.println("Config file : " + configFilePath);
            System.out.println(directories.directory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }
}
