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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import com.amazonaws.neptune.client.rdf4j.NeptuneSparqlRepository;
import com.amazonaws.services.neptune.cluster.ConnectionConfig;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.rdf.io.RdfTargetConfig;
import com.amazonaws.services.neptune.util.EnvironmentVariableUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.eclipse.rdf4j.http.client.HttpClientSessionManager;
import org.eclipse.rdf4j.http.client.RDF4JProtocolSession;
import org.eclipse.rdf4j.http.client.SPARQLProtocolSession;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.base.AbstractRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class NeptuneSparqlClient implements AutoCloseable {

    private static final ParserConfig PARSER_CONFIG = new ParserConfig().addNonFatalError(BasicParserSettings.VERIFY_URI_SYNTAX);

    public static NeptuneSparqlClient create(ConnectionConfig config) {
        if (config.useIamAuth()) {
            String serviceRegion = EnvironmentVariableUtils.getMandatoryEnv("SERVICE_REGION");
            AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            return new NeptuneSparqlClient(
                    config.endpoints().stream().map(e -> {
                        try {
                            return updateParser(new NeptuneSparqlRepository(sparqlEndpount(e, config.port()), credentialsProvider, serviceRegion));
                        } catch (NeptuneSigV4SignerException e1) {
                            throw new RuntimeException(e1);
                        }
                    }).
                            peek(AbstractRepository::init).
                            collect(Collectors.toList()));
        } else {

            return new NeptuneSparqlClient(
                    config.endpoints().stream().map(e ->
                            updateParser(new SPARQLRepository(sparqlEndpount(e, config.port())))).
                            peek(AbstractRepository::init).
                            collect(Collectors.toList()));
        }
    }

    private static SPARQLRepository updateParser(SPARQLRepository repository) {

        HttpClientSessionManager sessionManager = repository.getHttpClientSessionManager();
        repository.setHttpClientSessionManager(new HttpClientSessionManager() {
            @Override
            public HttpClient getHttpClient() {
                return sessionManager.getHttpClient();
            }

            @Override
            public SPARQLProtocolSession createSPARQLProtocolSession(String s, String s1) {
                SPARQLProtocolSession session = sessionManager.createSPARQLProtocolSession(s, s1);
                session.setParserConfig(PARSER_CONFIG);
                return session;
            }

            @Override
            public RDF4JProtocolSession createRDF4JProtocolSession(String s) {
                return sessionManager.createRDF4JProtocolSession(s);
            }

            @Override
            public void shutDown() {
                sessionManager.shutDown();
            }
        });
        return repository;
    }

    private static String sparqlEndpount(String endpoint, int port) {
        return String.format("https://%s:%s/sparql", endpoint, port);
    }

    private final List<SPARQLRepository> repositories;
    private final Random random = new Random(DateTime.now().getMillis());

    private NeptuneSparqlClient(List<SPARQLRepository> repositories) {
        this.repositories = repositories;
    }

    public void executeQuery(String sparql, RdfTargetConfig targetConfig) throws IOException {
        SPARQLRepository repository = chooseRepository();
        ValueFactory factory = repository.getValueFactory();

        try (RepositoryConnection connection = repository.getConnection();
             OutputWriter outputWriter = targetConfig.createOutputWriter()) {

            RDFWriter writer = targetConfig.createRDFWriter(outputWriter);

            connection.prepareTupleQuery(sparql).evaluate(new TupleQueryResultHandler() {
                @Override
                public void handleBoolean(boolean value) throws QueryResultHandlerException {

                }

                @Override
                public void handleLinks(List<String> linkUrls) throws QueryResultHandlerException {

                }

                @Override
                public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
                    writer.startRDF();
                }

                @Override
                public void endQueryResult() throws TupleQueryResultHandlerException {
                    writer.endRDF();
                }

                @Override
                public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                    Value s = bindingSet.getValue("s");
                    Value p = bindingSet.getValue("p");
                    Value o = bindingSet.getValue("o");
                    Value g = bindingSet.getValue("g");


                    IRI subject = factory.createIRI(s.stringValue());
                    IRI predicate = factory.createIRI(p.stringValue());
                    IRI graph = getNonDefaultNamedGraph(g, factory);

                    Statement statement = factory.createStatement(subject, predicate, o, graph);

                    writer.handleStatement(statement);
                }

            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IRI getNonDefaultNamedGraph(Value g, ValueFactory factory) {
        String s = g.stringValue();

        if (StringUtils.isEmpty(s) || s.equalsIgnoreCase("http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph")){
            return null;
        }

        return factory.createIRI(s);
    }

    private SPARQLRepository chooseRepository() {
        return repositories.get(random.nextInt(repositories.size()));
    }

    @Override
    public void close() {
        repositories.forEach(AbstractRepository::shutDown);
    }
}
