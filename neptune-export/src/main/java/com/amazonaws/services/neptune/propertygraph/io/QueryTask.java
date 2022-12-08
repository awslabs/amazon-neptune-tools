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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.NamedQuery;
import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.util.Activity;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTask implements Callable<Object> {

    private static final Logger logger = LoggerFactory.getLogger(QueryTask.class);

    private final Queue<NamedQuery> queries;
    private final NeptuneGremlinClient.QueryClient queryClient;
    private final PropertyGraphTargetConfig targetConfig;
    private final boolean twoPassAnalysis;
    private final Long timeoutMillis;
    private final Status status;
    private final AtomicInteger index;

    public QueryTask(Queue<NamedQuery> queries,
                     NeptuneGremlinClient.QueryClient queryClient,
                     PropertyGraphTargetConfig targetConfig,
                     boolean twoPassAnalysis,
                     Long timeoutMillis,
                     Status status,
                     AtomicInteger index) {

        this.queries = queries;
        this.queryClient = queryClient;
        this.targetConfig = targetConfig;
        this.twoPassAnalysis = twoPassAnalysis;
        this.timeoutMillis = timeoutMillis;
        this.status = status;
        this.index = index;
    }

    @Override
    public Object call() throws Exception {

        QueriesWriterFactory writerFactory = new QueriesWriterFactory();
        Map<Label, LabelWriter<Map<?, ?>>> labelWriters = new HashMap<>();

        try {

            while (status.allowContinue()) {

                try {

                    NamedQuery namedQuery = queries.poll();

                    if (!(namedQuery == null)) {

                        final GraphElementSchemas graphElementSchemas = new GraphElementSchemas();

                        if (twoPassAnalysis) {
                            Timer.timedActivity(String.format("generating schema for query [%s]", namedQuery.query()),
                                    (Activity.Runnable) () -> updateSchema(namedQuery, graphElementSchemas));
                        }

                        Timer.timedActivity(String.format("executing query [%s]", namedQuery.query()),
                                (CheckedActivity.Runnable) () ->
                                        executeQuery(namedQuery, writerFactory, labelWriters, graphElementSchemas));

                    } else {
                        status.halt();
                    }

                } catch (IllegalStateException e) {
                    logger.warn("Unexpected result value. {}. Proceeding with next query.", e.getMessage());
                }
            }

        } finally {
            for (LabelWriter<Map<?, ?>> labelWriter : labelWriters.values()) {
                try {
                    labelWriter.close();
                } catch (Exception e) {
                    logger.warn("Error closing label writer: {}.", e.getMessage());
                }
            }
        }

        return null;

    }

    private void updateSchema(NamedQuery namedQuery, GraphElementSchemas graphElementSchemas) {
        ResultSet firstPassResults = queryClient.submit(namedQuery.query(), timeoutMillis);

        firstPassResults.stream().
                map(r -> castToMap(r.getObject())).
                forEach(r -> {
                    graphElementSchemas.update(new Label(namedQuery.name()), r, true);
                });
    }

    private void executeQuery(NamedQuery namedQuery,
                              QueriesWriterFactory writerFactory,
                              Map<Label, LabelWriter<Map<?, ?>>> labelWriters,
                              GraphElementSchemas graphElementSchemas) {

        ResultSet results = queryClient.submit(namedQuery.query(), timeoutMillis);

        ResultsHandler resultsHandler = new ResultsHandler(
                new Label(namedQuery.name()),
                labelWriters,
                writerFactory,
                graphElementSchemas);

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

    private HashMap<?, ?> castToMap(Object o) {
        if (Map.class.isAssignableFrom(o.getClass())) {
            return (HashMap<?, ?>) o;
        }

        throw new IllegalStateException("Expected Map, found " + o.getClass().getSimpleName());
    }

    private class ResultsHandler implements GraphElementHandler<Map<?, ?>> {

        private final Label label;
        private final Map<Label, LabelWriter<Map<?, ?>>> labelWriters;
        private final QueriesWriterFactory writerFactory;
        private final GraphElementSchemas graphElementSchemas;

        private ResultsHandler(Label label,
                               Map<Label, LabelWriter<Map<?, ?>>> labelWriters,
                               QueriesWriterFactory writerFactory,
                               GraphElementSchemas graphElementSchemas) {
            this.label = label;
            this.labelWriters = labelWriters;
            this.writerFactory = writerFactory;

            this.graphElementSchemas = graphElementSchemas;
        }

        private void createWriter(Map<?, ?> properties, boolean allowStructuralElements) {
            try {

                if (!graphElementSchemas.hasSchemaFor(label)) {
                    graphElementSchemas.update(label, properties, allowStructuralElements);
                }

                LabelSchema labelSchema = graphElementSchemas.getSchemaFor(label);
                PropertyGraphPrinter propertyGraphPrinter =
                        writerFactory.createPrinter(Directories.fileName(label.fullyQualifiedLabel(), index), labelSchema, targetConfig);

                labelWriters.put(label, writerFactory.createLabelWriter(propertyGraphPrinter, label));

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void handle(Map<?, ?> properties, boolean allowTokens) throws IOException {

            if (!labelWriters.containsKey(label)) {
                createWriter(properties, allowTokens);
            }

            labelWriters.get(label).handle(properties, allowTokens);
        }

        @Override
        public void close() throws Exception {
            // Do nothing
        }
    }

    private static class StatusHandler implements GraphElementHandler<Map<?, ?>> {

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
