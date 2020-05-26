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
    private final PropertyGraphTargetConfig targetConfig;
    private final RangeFactory rangeFactory;
    private final Status status;
    private final int index;
    private final Map<String, GraphElementHandler<T>> labelWriters = new HashMap<>();

    public ExportPropertyGraphTask(PropertiesMetadata propertiesMetadata,
                                   LabelsFilter labelsFilter,
                                   GraphClient<T> graphClient,
                                   WriterFactory<T> writerFactory,
                                   PropertyGraphTargetConfig targetConfig,
                                   RangeFactory rangeFactory,
                                   Status status,
                                   int index) {
        this.propertiesMetadata = propertiesMetadata;
        this.labelsFilter = labelsFilter;
        this.graphClient = graphClient;
        this.writerFactory = writerFactory;
        this.targetConfig = targetConfig;
        this.rangeFactory = rangeFactory;
        this.status = status;
        this.index = index;
    }

    @Override
    public void run() {
        try {
            while (status.allowContinue()) {
                Range range = rangeFactory.nextRange();
                if (range.isEmpty()) {
                    status.halt();
                } else {
                    CountingHandler handler = new CountingHandler(this);

                    //labelsFilter.getPropertiesForLabels(propertiesMetadata);

                    graphClient.queryForValues(handler, range, labelsFilter, propertiesMetadata);

                    if (range.sizeExceeds(handler.numberProcessed()) || rangeFactory.isExhausted()) {
                        status.halt();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void handle(T input, boolean allowTokens) throws IOException {
        status.update();
        String label = graphClient.getLabelsAsStringToken(input);
        if (!labelWriters.containsKey(label)) {
            createWriterFor(label);
        }
        graphClient.updateStats(label);
        labelWriters.get(label).handle(input, allowTokens);
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

            PropertyGraphPrinter propertyGraphPrinter = writerFactory.createPrinter(label, index, propertyMetadata, targetConfig);
            propertyGraphPrinter.printHeaderRemainingColumns(propertyMetadata.values());

            labelWriters.put(label, writerFactory.createLabelWriter(propertyGraphPrinter));

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
        public void handle(T input, boolean allowTokens) throws IOException {
            parent.handle(input, allowTokens);
            counter++;
        }

        long numberProcessed() {
            return counter;
        }

        @Override
        public void close() throws Exception {
            parent.close();
        }
    }
}
