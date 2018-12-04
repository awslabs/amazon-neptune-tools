package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.graph.ConcurrencyConfig;
import com.amazonaws.services.neptune.graph.NeptuneClient;
import com.amazonaws.services.neptune.graph.Scope;
import com.amazonaws.services.neptune.io.ExportJob;
import com.amazonaws.services.neptune.io.Format;
import com.amazonaws.services.neptune.metadata.CreateMetadataFromConfigFile;
import com.amazonaws.services.neptune.metadata.MetadataSpecification;
import com.amazonaws.services.neptune.metadata.PropertiesMetadataCollection;
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
        "bin/neptune-export.sh export-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output",
        "bin/neptune-export.sh export-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output --format json"
}, descriptions ={
        "Export data using the metadata config in /home/ec2-user/config.json",
        "Export data as JSON using the metadata config in /home/ec2-user/config.json"
})
@Command(name = "export-from-config", description = "Export from Neptune to CSV or JSON using an existing config file")
public class ExportFromConfig implements Runnable {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint")
    @Required
    @Once
    private String endpoint;

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

    @Option(name = {"-c", "--config-file"}, description = "Path to JSON config file")
    @Required
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File configFile;

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
    @AllowedValues(allowedValues = { "all", "nodes", "edges" })
    private Scope scope = Scope.all;

    @Option(name = {"--format"}, description = "Output format (optional, default 'csv')")
    @Once
    @AllowedValues(allowedValues = {"csv", "json"})
    private Format format = Format.csv;

    @Override
    public void run() {
        ConcurrencyConfig concurrencyConfig = new ConcurrencyConfig(concurrency, range);

        try (Timer timer = new Timer();
             NeptuneClient client = NeptuneClient.create(endpoint, port, concurrencyConfig);
             GraphTraversalSource g = client.newTraversalSource()) {

            Directories directories = Directories.createFor(directory, tag);
            PropertiesMetadataCollection metadataCollection = new CreateMetadataFromConfigFile(configFile).execute();

            Collection<MetadataSpecification<?>> metadataSpecifications = scope.metadataSpecifications(nodeLabels, edgeLabels);

            ExportJob exportJob = new ExportJob(metadataSpecifications, metadataCollection, g, concurrencyConfig, directories, format);
            exportJob.execute();

            System.err.println(format.description() + " files   : " + directories.directory());
            System.err.println("Config file : " + configFile.getAbsolutePath());
            System.out.println(directories.directory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }
}
