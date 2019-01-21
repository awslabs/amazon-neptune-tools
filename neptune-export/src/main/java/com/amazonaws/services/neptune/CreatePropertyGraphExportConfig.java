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

import com.amazonaws.services.neptune.propertygraph.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.MetadataSamplingSpecification;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.Scope;
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
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output",
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --sample --sample-size 100",
        "bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS"
}, descriptions = {
        "Create metadata config file for all node and edge labels and save it to /home/ec2-user/output",
        "Create metadata config file for all node and edge labels, sampling 100 nodes and edges for each label",
        "Create config file containing metadata for User nodes and FOLLOWS edges"
})
@Command(name = "create-pg-config", description = "Create a property graph export metadata config file")
public class CreatePropertyGraphExportConfig implements Runnable {

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

    @Option(name = {"-nl", "--node-label"}, description = "Labels of nodes to be included in config (optional, default all labels)",
            arity = 1)
    private List<String> nodeLabels = new ArrayList<>();

    @Option(name = {"-el", "--edge-label"}, description = "Labels of edges to be included in config (optional, default all labels)",
            arity = 1)
    private List<String> edgeLabels = new ArrayList<>();

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

    @Override
    public void run() {
        ConcurrencyConfig concurrencyConfig = new ConcurrencyConfig(1, -1);
        MetadataSamplingSpecification metadataSamplingSpecification = new MetadataSamplingSpecification(sample, sampleSize);

        try (Timer timer = new Timer();
             NeptuneGremlinClient client = NeptuneGremlinClient.create(endpoints, port, concurrencyConfig, useIamAuth);
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = Directories.createFor(directory, tag);
            java.nio.file.Path configFilePath = directories.configFilePath();

            Collection<MetadataSpecification<?>> metadataSpecifications = scope.metadataSpecifications(nodeLabels, edgeLabels);

            MetadataCommand metadataCommand = metadataSamplingSpecification.createMetadataCommand(metadataSpecifications, g);
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
