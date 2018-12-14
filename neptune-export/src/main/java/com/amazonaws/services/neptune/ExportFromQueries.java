package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.airline.NameQueriesTypeConverter;
import com.amazonaws.services.neptune.graph.ConcurrencyConfig;
import com.amazonaws.services.neptune.graph.NamedQueries;
import com.amazonaws.services.neptune.graph.NamedQueriesCollection;
import com.amazonaws.services.neptune.graph.NeptuneClient;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.Format;
import com.amazonaws.services.neptune.io.QueryJob;
import com.amazonaws.services.neptune.metadata.CreateQueriesFromFile;
import com.amazonaws.services.neptune.metadata.SaveQueries;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Examples(examples = {
        "bin/neptune-export.sh export-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person=\"g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')\" -q post=\"g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())\" --concurrency 6",
        "bin/neptune-export.sh export-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person=\"g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')\" -q post=\"g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())\" --concurrency 6 --format json"},

        descriptions = {
                "Parallel export of Person data in 2 shards, sharding on the 'birthday' property, and Post data in 4 shards, sharding on range, using 6 threads",
                "Parallel export of Person data and Post data as JSON"
        })
@Command(name = "export-from-queries", description = "Export to CSV or JSON from Gremlin queries")
public class ExportFromQueries implements Runnable {
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

    @Option(name = {"-cn", "--concurrency"}, description = "Concurrency (optional)")
    @Once
    private int concurrency = 1;

    @Option(name = {"--use-iam-auth"}, description = "Use IAM database authentication to authenticate to Neptune")
    @Once
    private boolean useIamAuth = false;

    @Option(name = {"-q", "--queries"}, description = "Gremlin queries (format: name=\"semi-colon-separated list of queries\")",
            arity = 1, typeConverterProvider = NameQueriesTypeConverter.class)
    private List<NamedQueries> queries = new ArrayList<>();

    @Option(name = {"-b", "--batch-size"}, description = "Batch size (optional, default 64). Reduce this number if your queries trigger CorruptedFrameExceptions.")
    @Once
    private int batchSize = NeptuneClient.DEFAULT_BATCH_SIZE;

    @Option(name = {"-f", "--queries-file"}, description = "Path to JSON queries file")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File queriesFile;

    @Option(name = {"--format"}, description = "Output format (optional, default 'csv')")
    @Once
    @AllowedValues(allowedValues = {"csv", "json"})
    private Format format = Format.csv;

    @Override
    public void run() {
        ConcurrencyConfig concurrencyConfig = new ConcurrencyConfig(concurrency, -1);

        try (Timer timer = new Timer();
             NeptuneClient client = NeptuneClient.create(endpoints, port, concurrencyConfig, batchSize, useIamAuth);
             NeptuneClient.QueryClient queryClient = client.queryClient()) {

            Directories directories = Directories.createFor(directory, tag);

            QueriesInfo queriesInfo = getNamedQueriesCollection(queries, queriesFile, directories);

            directories.createSubdirectories(
                    directories.resultsDirectory(),
                    queriesInfo.namedQueriesCollection().names());

            QueryJob queryJob = new QueryJob(
                    queriesInfo.namedQueriesCollection().flatten(),
                    queryClient, concurrencyConfig,
                    directories, format);
            queryJob.execute();

            System.err.println("CSV files   : " + directories.resultsDirectory());
            System.err.println("Queries file : " + queriesInfo.queriesFile());
            System.out.println(directories.resultsDirectory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }

    }

    private QueriesInfo getNamedQueriesCollection(List<NamedQueries> queries,
                                                  File queriesFile,
                                                  Directories directories) throws IOException {
        if (queriesFile == null) {
            NamedQueriesCollection namedQueries = new NamedQueriesCollection(queries);
            new SaveQueries(namedQueries, directories.queriesFilePath()).execute();
            return new QueriesInfo(namedQueries, directories.queriesFilePath());
        } else {
            NamedQueriesCollection namedQueries = new CreateQueriesFromFile(queriesFile).execute();
            return new QueriesInfo(namedQueries, queriesFile.toPath());
        }
    }

    private static class QueriesInfo {
        private final NamedQueriesCollection namedQueriesCollection;
        private final java.nio.file.Path queriesFile;

        private QueriesInfo(NamedQueriesCollection namedQueriesCollection, java.nio.file.Path queriesFile) {
            this.namedQueriesCollection = namedQueriesCollection;
            this.queriesFile = queriesFile;
        }

        public NamedQueriesCollection namedQueriesCollection() {
            return namedQueriesCollection;
        }

        public java.nio.file.Path queriesFile() {
            return queriesFile;
        }
    }
}
