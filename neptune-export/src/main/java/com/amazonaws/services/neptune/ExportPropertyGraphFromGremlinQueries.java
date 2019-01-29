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

import com.amazonaws.services.neptune.auth.HandshakeRequestConfig;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.propertygraph.airline.NameQueriesTypeConverter;
import com.amazonaws.services.neptune.propertygraph.ConcurrencyConfig;
import com.amazonaws.services.neptune.propertygraph.NamedQueries;
import com.amazonaws.services.neptune.propertygraph.NamedQueriesCollection;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.io.Format;
import com.amazonaws.services.neptune.propertygraph.io.QueryJob;
import com.amazonaws.services.neptune.propertygraph.metadata.CreateQueriesFromFile;
import com.amazonaws.services.neptune.propertygraph.metadata.SaveQueries;
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
        "bin/neptune-export.sh export-pg-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person=\"g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')\" -q post=\"g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())\" --concurrency 6",
        "bin/neptune-export.sh export-pg-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person=\"g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')\" -q post=\"g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())\" --concurrency 6 --format json"},

        descriptions = {
                "Parallel export of Person data in 2 shards, sharding on the 'birthday' property, and Post data in 4 shards, sharding on range, using 6 threads",
                "Parallel export of Person data and Post data as JSON"
        })
@Command(name = "export-pg-from-queries", description = "Export property graph to CSV or JSON from Gremlin queries")
public class ExportPropertyGraphFromGremlinQueries extends NeptuneExportBaseCommand implements Runnable {

    @Option(name = {"-cn", "--concurrency"}, description = "Concurrency (optional)")
    @Once
    private int concurrency = 1;

    @Option(name = {"-q", "--queries"}, description = "Gremlin queries (format: name=\"semi-colon-separated list of queries\")",
            arity = 1, typeConverterProvider = NameQueriesTypeConverter.class)
    private List<NamedQueries> queries = new ArrayList<>();

    @Option(name = {"-b", "--batch-size"}, description = "Batch size (optional, default 64). Reduce this number if your queries trigger CorruptedFrameExceptions.")
    @Once
    private int batchSize = NeptuneGremlinClient.DEFAULT_BATCH_SIZE;

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
             NeptuneGremlinClient client = NeptuneGremlinClient.create(connectionConfig(), concurrencyConfig, batchSize);
             NeptuneGremlinClient.QueryClient queryClient = client.queryClient()) {

            Directories directories = Directories.createFor(DirectoryStructure.GremlinQueries, directory, tag);

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
