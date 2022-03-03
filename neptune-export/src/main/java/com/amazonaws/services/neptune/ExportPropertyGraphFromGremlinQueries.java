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
import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.NamedQueries;
import com.amazonaws.services.neptune.propertygraph.NamedQueriesCollection;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.airline.NameQueriesTypeConverter;
import com.amazonaws.services.neptune.propertygraph.io.*;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.Once;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Examples(examples = {
        "bin/neptune-export.sh export-pg-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person=\"g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')\" -q post=\"g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())\" --concurrency 6",
        "bin/neptune-export.sh export-pg-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person=\"g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')\" -q post=\"g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())\" --concurrency 6 --format json"},

        descriptions = {
                "Parallel export of Person data in 2 shards, sharding on the 'birthday' property, and Post data in 4 shards, sharding on range, using 6 threads",
                "Parallel export of Person data and Post data as JSON"
        })
@Command(name = "export-pg-from-queries", description = "Export property graph to CSV or JSON from Gremlin queries.")
public class ExportPropertyGraphFromGremlinQueries extends NeptuneExportCommand implements Runnable {

    @Inject
    private CloneClusterModule cloneStrategy = new CloneClusterModule(awsCli);

    @Inject
    private CommonConnectionModule connection = new CommonConnectionModule(awsCli);

    @Inject
    private PropertyGraphTargetModule target = new PropertyGraphTargetModule();

    @Inject
    private PropertyGraphConcurrencyModule concurrency = new PropertyGraphConcurrencyModule();

    @Inject
    private PropertyGraphSerializationModule serialization = new PropertyGraphSerializationModule();

    @Option(name = {"-q", "--queries", "--query", "--gremlin"}, description = "Gremlin queries (format: name=\"semi-colon-separated list of queries\" OR \"semi-colon-separated list of queries\").",
            arity = 1, typeConverterProvider = NameQueriesTypeConverter.class)
    private List<NamedQueries> queries = new ArrayList<>();

    @Option(name = {"-f", "--queries-file"}, description = "Path to JSON queries file (file path, or 'https' or 's3' URI).")
    @Once
    private URI queriesFile;

    @Option(name = {"--two-pass-analysis"}, description = "Perform two-pass analysis of query results (optional, default 'false').")
    @Once
    private boolean twoPassAnalysis = false;

    @Option(name = {"--include-type-definitions"}, description = "Include type definitions from column headers (optional, default 'false').")
    @Once
    private boolean includeTypeDefinitions = false;

    @Option(name = {"--strict-cardinality"}, description = "Use strict cardinality (optional, default 'false').")
    @Once
    private boolean strictCardinality = false;

    @Option(name = {"--timeout-millis"}, description = "Query timeout in milliseconds (optional).")
    @Once
    private Long timeoutMillis = null;

    @Override
    public void run() {

        try {
            Timer.timedActivity("exporting property graph from queries", (CheckedActivity.Runnable) () -> {
                try (Cluster cluster = cloneStrategy.cloneCluster(connection.config(), concurrency.config(), featureToggles())) {

                    Directories directories = target.createDirectories(DirectoryStructure.GremlinQueries);
                    JsonResource<NamedQueriesCollection> queriesResource = queriesFile != null ?
                            new JsonResource<>("Queries file", queriesFile, NamedQueriesCollection.class) :
                            directories.queriesResource();

                    CsvPrinterOptions csvPrinterOptions = CsvPrinterOptions.builder().setIncludeTypeDefinitions(includeTypeDefinitions).build();
                    JsonPrinterOptions jsonPrinterOptions = JsonPrinterOptions.builder().setStrictCardinality(strictCardinality).build();

                    PropertyGraphTargetConfig targetConfig = target.config(directories, new PrinterOptions(csvPrinterOptions, jsonPrinterOptions));
                    NamedQueriesCollection namedQueries = getNamedQueriesCollection(queries, queriesFile, queriesResource);

                    directories.createResultsSubdirectories(namedQueries.names());

                    try (NeptuneGremlinClient client = NeptuneGremlinClient.create(cluster, serialization.config());
                         NeptuneGremlinClient.QueryClient queryClient = client.queryClient()) {

                        QueryJob queryJob = new QueryJob(
                                namedQueries.flatten(),
                                queryClient,
                                cluster.concurrencyConfig(),
                                targetConfig,
                                twoPassAnalysis,
                                timeoutMillis);
                        queryJob.execute();

                    }

                    directories.writeResultsDirectoryPathAsMessage(target.description(), target);

                    queriesResource.writeResourcePathAsMessage(target);

                    directories.writeRootDirectoryPathAsReturnValue(target);
                    onExportComplete(directories, new ExportStats(), cluster);

                }
            });
        } catch (Exception e) {
            handleException(e);
        }
    }

    private NamedQueriesCollection getNamedQueriesCollection(List<NamedQueries> queries,
                                                             URI queriesFile,
                                                             JsonResource<NamedQueriesCollection> queriesResource) throws IOException {
        if (queriesFile == null) {
            NamedQueriesCollection namedQueries = new NamedQueriesCollection(queries);
            queriesResource.save(namedQueries);
            return namedQueries;
        } else {
            return queriesResource.get();
        }
    }
}
