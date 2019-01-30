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
import com.amazonaws.services.neptune.propertygraph.GraphClient;
import com.amazonaws.services.neptune.propertygraph.LabelsFilter;
import com.amazonaws.services.neptune.propertygraph.Range;
import com.amazonaws.services.neptune.propertygraph.RangeFactory;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadata;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExportPropertyGraphTask<T> implements Runnable, GraphElementHandler<T> {

    private final PropertiesMetadata propertiesMetadata;
    private final LabelsFilter labelsFilter;
    private final GraphClient<T> graphClient;
    private final WriterFactory<T> writerFactory;
    private final Format format;
    private final RangeFactory rangeFactory;
    private final Status status;
    private final int index;
    private final Map<String, GraphElementHandler<T>> labelWriters = new HashMap<>();

    public ExportPropertyGraphTask(PropertiesMetadata propertiesMetadata,
                                   LabelsFilter labelsFilter,
                                   GraphClient<T> graphClient,
                                   WriterFactory<T> writerFactory,
                                   Format format, RangeFactory rangeFactory,
                                   Status status,
                                   int index) {
        this.propertiesMetadata = propertiesMetadata;
        this.labelsFilter = labelsFilter;
        this.graphClient = graphClient;
        this.writerFactory = writerFactory;
        this.format = format;
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
    public void handle(T input, boolean allowStructuralElements) throws IOException {
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
            Map<Object, PropertyTypeInfo> propertyMetadata = propertiesMetadata.propertyMetadataFor(label);

            if (propertyMetadata == null) {
                System.err.printf("%nWARNING: Unable to find property metadata for '%s' %s label%n", label, graphClient.description());
                propertyMetadata = new HashMap<>();
            }

            Printer printer = writerFactory.createPrinter(label, index, propertyMetadata, format);
            printer.printHeaderRemainingColumns(propertyMetadata.values(), true);

            labelWriters.put(label, writerFactory.createLabelWriter(printer));

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
        public void handle(T input, boolean allowStructuralElements) throws IOException {
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
