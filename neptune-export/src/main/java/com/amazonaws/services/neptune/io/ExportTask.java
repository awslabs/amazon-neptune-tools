package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.graph.GraphClient;
import com.amazonaws.services.neptune.graph.LabelsFilter;
import com.amazonaws.services.neptune.graph.Range;
import com.amazonaws.services.neptune.graph.RangeFactory;
import com.amazonaws.services.neptune.metadata.PropertiesMetadata;
import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ExportTask<T> implements Runnable, GraphElementHandler<T> {

    private final PropertiesMetadata propertiesMetadata;
    private final LabelsFilter labelsFilter;
    private final GraphClient<T> graphClient;
    private final WriterFactory<T> writerFactory;
    private final RangeFactory rangeFactory;
    private final Status status;
    private final int index;
    private final Map<String, GraphElementHandler<T>> labelWriters = new HashMap<>();

    public ExportTask(PropertiesMetadata propertiesMetadata,
                      LabelsFilter labelsFilter,
                      GraphClient<T> graphClient,
                      WriterFactory<T> writerFactory,
                      RangeFactory rangeFactory,
                      Status status,
                      int index) {
        this.propertiesMetadata = propertiesMetadata;
        this.labelsFilter = labelsFilter;
        this.graphClient = graphClient;
        this.writerFactory = writerFactory;
        this.rangeFactory = rangeFactory;
        this.status = status;
        this.index = index;
    }

    @Override
    public void run() {
        try {
            while (status.allowContinue()) {
                Range range = rangeFactory.nextRange();
                if (rangeFactory.exceedsUpperBound(range)) {
                    status.halt();
                } else {
                    CountingHandler handler = new CountingHandler(this);
                    graphClient.queryForValues(handler, range, labelsFilter);
                    if (range.value() > handler.counter()) {
                        status.halt();
                    }
                }
            }
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(T input, boolean allowStructuralElements) {
        status.update();
        String label = graphClient.getLabelFrom(input);
        if (!labelWriters.containsKey(label)) {
            createWriterFor(label);
        }
        labelWriters.get(label).handle(input, allowStructuralElements);
    }

    @Override
    public void close() throws Exception {
        for (GraphElementHandler<T> labelWriter : labelWriters.values()) {
            labelWriter.close();
        }
    }

    private void createWriterFor(String label) {
        try {
            Map<String, PropertyTypeInfo> propertyMetadata = propertiesMetadata.propertyMetadataFor(label);

            if (propertyMetadata == null) {
                System.err.printf("%nWARNING: Unable to find property metadata for '%s' %s label%n", label, graphClient.description());
                propertyMetadata = new HashMap<>();
            }

            PrintWriter printer = writerFactory.createPrinter(label, index);
            PropertyWriter propertyWriter = new PropertyWriter(propertyMetadata, true);

            writerFactory.printHeader(printer);
            for (PropertyTypeInfo property : propertyMetadata.values()) {
                printer.printf(",%s", property.nameWithDataType());
            }
            printer.print(System.lineSeparator());

            labelWriters.put(label, writerFactory.createLabelWriter(printer, propertyWriter));


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class CountingHandler implements GraphElementHandler<T> {

        private final GraphElementHandler<T> parent;
        private long counter = 0;

        private CountingHandler(GraphElementHandler<T> parent) {
            this.parent = parent;
        }

        @Override
        public void handle(T input, boolean allowStructuralElements) {
            parent.handle(input, allowStructuralElements);
            counter++;
        }

        long counter() {
            return counter;
        }

        @Override
        public void close() throws Exception {
            parent.close();
        }
    }
}
