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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.propertygraph.NamedQuery;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadata;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.omg.CORBA.INTERNAL;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class QueryTask implements Runnable {
    private final Queue<NamedQuery> queries;
    private final NeptuneGremlinClient.QueryClient queryClient;
    private final PropertyGraphTargetConfig targetConfig;
    private final boolean twoPassAnalysis;
    private final Status status;
    private final int index;

    public QueryTask(Queue<NamedQuery> queries,
                     NeptuneGremlinClient.QueryClient queryClient,
                     PropertyGraphTargetConfig targetConfig,
                     boolean twoPassAnalysis,
                     Status status,
                     int index) {

        this.queries = queries;
        this.queryClient = queryClient;
        this.targetConfig = targetConfig;
        this.twoPassAnalysis = twoPassAnalysis;
        this.status = status;
        this.index = index;
    }

    @Override
    public void run() {

        QueriesWriterFactory writerFactory = new QueriesWriterFactory();
        Map<String, GraphElementHandler<Map<?, ?>>> labelWriters = new HashMap<>();

        try {

            while (status.allowContinue()) {

                try {

                    NamedQuery namedQuery = queries.poll();
                    if (!(namedQuery == null)) {

                        PropertiesMetadata propertiesMetadata = new PropertiesMetadata();

                        ResultSet results = queryClient.submit(namedQuery.query());

                        if (twoPassAnalysis) {
                            // First pass
                            results.stream().
                                    map(r -> castToMap(r.getObject())).
                                    forEach(r -> {
                                        propertiesMetadata.update(namedQuery.name(), r, true);
                                    });

                            // Re-run query for second pass
                            results = queryClient.submit(namedQuery.query());
                        }

                        try (Timer timer = new Timer(String.format("query [%s]", namedQuery.query()))) {

                            ResultsHandler resultsHandler = new ResultsHandler(
                                    namedQuery.name(), labelWriters, writerFactory, propertiesMetadata);
                            StatusHandler handler = new StatusHandler(resultsHandler, status);

                            results.stream().
                                    map(r -> castToMap(r.getObject())).
                                    forEach(r -> {
                                        try {
                                            handler.handle(r, true);
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                        }

                    } else {
                        status.halt();
                    }

                } catch (IllegalStateException e) {
                    System.err.printf("%nWARNING: Unexpected result value. %s. Proceeding with next query.%n", e.getMessage());
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                for (GraphElementHandler<Map<?, ?>> labelWriter : labelWriters.values()) {
                    labelWriter.close();
                }
            } catch (Exception e) {
                System.err.printf("%nWARNING: Error closing writer: %s%n", e.getMessage());
            }
        }

    }

    private HashMap<?, ?> castToMap(Object o) {
        if (Map.class.isAssignableFrom(o.getClass())) {
            return (HashMap<?, ?>) o;
        }

        throw new IllegalStateException("Expected Map, found " + o.getClass().getSimpleName());
    }

    private class ResultsHandler implements GraphElementHandler<Map<?, ?>> {

        private final String name;
        private final Map<String, GraphElementHandler<Map<?, ?>>> labelWriters;
        private final QueriesWriterFactory writerFactory;
        private final PropertiesMetadata propertiesMetadata;

        private ResultsHandler(String name,
                               Map<String, GraphElementHandler<Map<?, ?>>> labelWriters,
                               QueriesWriterFactory writerFactory,
                               PropertiesMetadata propertiesMetadata) {
            this.name = name;
            this.labelWriters = labelWriters;
            this.writerFactory = writerFactory;

            this.propertiesMetadata = propertiesMetadata;
        }

        private void createWriter(Map<?, ?> properties, boolean allowStructuralElements) {
            try {

                if (!propertiesMetadata.hasMetadataFor(name)) {
                    propertiesMetadata.update(name, properties, allowStructuralElements);
                }

                Map<Object, PropertyTypeInfo> propertyMetadata = propertiesMetadata.propertyMetadataFor(name);
                PropertyGraphPrinter propertyGraphPrinter = writerFactory.createPrinter(name, index, propertyMetadata, targetConfig);

                propertyGraphPrinter.printHeaderRemainingColumns(propertyMetadata.values());

                labelWriters.put(name, writerFactory.createLabelWriter(propertyGraphPrinter));

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void handle(Map<?, ?> properties, boolean allowTokens) throws IOException {

            if (!labelWriters.containsKey(name)) {
                createWriter(properties, allowTokens);
            }

            labelWriters.get(name).handle(properties, allowTokens);
        }

        @Override
        public void close() throws Exception {
            // Do nothing
        }
    }

    private class StatusHandler implements GraphElementHandler<Map<?, ?>> {

        private final GraphElementHandler<Map<?, ?>> parent;
        private final Status status;

        private StatusHandler(GraphElementHandler<Map<?, ?>> parent, Status status) {
            this.parent = parent;
            this.status = status;
        }

        @Override
        public void handle(Map<?, ?> input, boolean allowTokens) throws IOException {
            parent.handle(input, allowTokens);
            status.update();
        }

        @Override
        public void close() throws Exception {
            parent.close();
        }
    }
}
