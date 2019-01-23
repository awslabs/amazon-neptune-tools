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

package com.amazonaws.services.neptune.rdf;

import com.amazonaws.services.neptune.rdf.io.EnhancedTurtleWriter;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.base.AbstractRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.joda.time.DateTime;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class NeptuneSparqlClient implements AutoCloseable {

    public static NeptuneSparqlClient create(Collection<String> endpoints, int port) {
        return new NeptuneSparqlClient(
                endpoints.stream().map(e ->
                        new SPARQLRepository(String.format("https://%s:%s/sparql", e, port))).
                        peek(r -> r.enableQuadMode(true)).
                        peek(AbstractRepository::initialize).
                        collect(Collectors.toList()));
    }

    private final List<SPARQLRepository> repositories;
    private final Random random = new Random(DateTime.now().getMillis());

    private NeptuneSparqlClient(List<SPARQLRepository> repositories) {
        this.repositories = repositories;
    }

    public void executeQuery(String sparql, Path file) throws IOException {
        Prefixes prefixes = new Prefixes();

        try (RepositoryConnection connection = chooseRepository().getConnection();
             Writer fileWriter = new FileWriter(file.toFile())) {

            EnhancedTurtleWriter writer = new EnhancedTurtleWriter(fileWriter, prefixes);
            connection.prepareGraphQuery(sparql).evaluate(writer);
        }

        prefixes.addTo(file);
    }

    private SPARQLRepository chooseRepository() {
        return repositories.get(random.nextInt(repositories.size()));
    }

    @Override
    public void close() throws Exception {
        repositories.forEach(AbstractRepository::shutDown);
    }
}
