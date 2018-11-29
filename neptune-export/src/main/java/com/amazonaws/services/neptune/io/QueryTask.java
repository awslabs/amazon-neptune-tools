package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.graph.NamedQuery;
import com.amazonaws.services.neptune.graph.NeptuneClient;
import com.amazonaws.services.neptune.metadata.PropertiesMetadata;
import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class QueryTask implements Runnable {
    private final Queue<NamedQuery> queries;
    private final NeptuneClient.QueryClient queryClient;
    private final Directories directories;
    private final Status status;
    private final int index;

    public QueryTask(Queue<NamedQuery> queries,
                     NeptuneClient.QueryClient queryClient,
                     Directories directories,
                     Status status,
                     int index) {

        this.queries = queries;
        this.queryClient = queryClient;
        this.directories = directories;
        this.status = status;
        this.index = index;
    }

    @Override
    public void run() {

        QueriesWriterFactory writerFactory = new QueriesWriterFactory(directories);
        PropertiesMetadata propertiesMetadata = new PropertiesMetadata();
        Map<String, GraphElementHandler<Map<?, ?>>> labelWriters = new HashMap<>();

        try {

            while (status.allowContinue()) {

                try {

                    NamedQuery namedQuery = queries.poll();
                    if (!(namedQuery == null)) {
                        ResultSet results = queryClient.submit(namedQuery.query());

                        ResultsHandler resultsHandler = new ResultsHandler(
                                namedQuery.name(), labelWriters, writerFactory, propertiesMetadata);
                        StatusHandler handler = new StatusHandler(resultsHandler, status);

                        results.stream().
                                map(r -> castToMap(r.getObject())).
                                forEach(r -> handler.handle(r, true));

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

                for (GraphElementHandler<Map<?, ?>> writer : labelWriters.values()) {
                    writer.close();
                }
            } catch (Exception e) {
                System.err.printf("%nWARNING: Error closing writer: %s%n", e.getMessage());
            }
        }

    }

    private HashMap<?, ?> castToMap(Object o){
        if (Map.class.isAssignableFrom(o.getClass())){
            return (HashMap<?, ?>)o;
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
                               QueriesWriterFactory writerFactory, PropertiesMetadata propertiesMetadata) {
            this.name = name;
            this.labelWriters = labelWriters;
            this.writerFactory = writerFactory;

            this.propertiesMetadata = propertiesMetadata;
        }

        private void createWriterFor(String name, Map<?, ?> properties, boolean allowStructuralElements) {
            try {
                if (!propertiesMetadata.hasMetadataFor(name)) {
                    propertiesMetadata.update(name, properties, allowStructuralElements);
                }

                Map<String, PropertyTypeInfo> propertyMetadata = propertiesMetadata.propertyMetadataFor(name);

                PrintWriter printer = writerFactory.createPrinter(name, index);
                PropertyWriter propertyWriter = new PropertyWriter(propertyMetadata, false);

                boolean printComma = false;

                for (PropertyTypeInfo property : propertyMetadata.values()) {
                    if (printComma){
                        printer.print(",");
                    }
                    else{
                        printComma = true;
                    }
                    printer.print(property.nameWithoutDataType());
                }
                printer.print(System.lineSeparator());

                labelWriters.put(name, writerFactory.createLabelWriter(printer, propertyWriter));


            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void handle(Map<?, ?> properties, boolean allowStructuralElements) {

            if (!labelWriters.containsKey(name)) {
                createWriterFor(name, properties, allowStructuralElements);
            }

            labelWriters.get(name).handle(properties, allowStructuralElements);
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
        public void handle(Map<?, ?> input, boolean allowStructuralElements) {
            parent.handle(input, allowStructuralElements);
            status.update();
        }

        @Override
        public void close() throws Exception {
            parent.close();
        }
    }
}
