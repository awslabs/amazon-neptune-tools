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
import com.amazonaws.services.neptune.propertygraph.schema.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ExportPropertyGraphTask<T extends Map<?, ?>> implements Callable<FileSpecificLabelSchemas> {

    private final LabelsFilter labelsFilter;
    private final GraphClient<T> graphClient;
    private final WriterFactory<T> writerFactory;
    private final PropertyGraphTargetConfig targetConfig;
    private final RangeFactory rangeFactory;
    private final Status status;
    private final int index;
    private final Map<String, LabelWriter<T>> labelWriters = new HashMap<>();
    private final GraphSchema graphSchema;
    private final GraphElementType<T> graphElementType;

    public ExportPropertyGraphTask(GraphSchema graphSchema,
                                   GraphElementType<T> graphElementType,
                                   LabelsFilter labelsFilter,
                                   GraphClient<T> graphClient,
                                   WriterFactory<T> writerFactory,
                                   PropertyGraphTargetConfig targetConfig,
                                   RangeFactory rangeFactory,
                                   Status status,
                                   int index) {
        this.graphSchema = graphSchema;
        this.graphElementType = graphElementType;
        this.labelsFilter = labelsFilter;
        this.graphClient = graphClient;
        this.writerFactory = writerFactory;
        this.targetConfig = targetConfig;
        this.rangeFactory = rangeFactory;
        this.status = status;
        this.index = index;
    }

    @Override
    public FileSpecificLabelSchemas call() {

        GraphElementSchemas graphElementSchemas =
                graphSchema.copyOfGraphElementSchemasFor(graphElementType);

        FileSpecificLabelSchemas fileSpecificLabelSchemas = new FileSpecificLabelSchemas();

        CountingHandler handler = new CountingHandler(
                new TaskHandler(
                        graphElementSchemas,
                        fileSpecificLabelSchemas,
                        targetConfig,
                        writerFactory,
                        labelWriters,
                        graphClient,
                        status,
                        index
                ));

        try {
            while (status.allowContinue()) {
                Range range = rangeFactory.nextRange();
                if (range.isEmpty()) {
                    status.halt();
                } else {
                    graphClient.queryForValues(handler, range, labelsFilter, graphElementSchemas);
                    if (range.sizeExceeds(handler.numberProcessed()) || rangeFactory.isExhausted()) {
                        status.halt();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                handler.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return fileSpecificLabelSchemas;
    }

    private class TaskHandler implements GraphElementHandler<T> {

        private final GraphElementSchemas graphElementSchemas;
        private final FileSpecificLabelSchemas fileSpecificLabelSchemas;
        private final PropertyGraphTargetConfig targetConfig;
        private final WriterFactory<T> writerFactory;
        private final Map<String, LabelWriter<T>> labelWriters;
        private final GraphClient<T> graphClient;
        private final Status status;
        private final int index;


        private TaskHandler(GraphElementSchemas graphElementSchemas,
                            FileSpecificLabelSchemas fileSpecificLabelSchemas,
                            PropertyGraphTargetConfig targetConfig,
                            WriterFactory<T> writerFactory,
                            Map<String, LabelWriter<T>> labelWriters,
                            GraphClient<T> graphClient,
                            Status status,
                            int index) {
            this.graphElementSchemas = graphElementSchemas;
            this.fileSpecificLabelSchemas = fileSpecificLabelSchemas;
            this.targetConfig = targetConfig;
            this.writerFactory = writerFactory;
            this.labelWriters = labelWriters;
            this.graphClient = graphClient;
            this.status = status;
            this.index = index;
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
            for (LabelWriter<T> labelWriter : labelWriters.values()) {
                labelWriter.close();
            }
        }

        private void createWriterFor(String label) {
            try {

                LabelSchema labelSchema = graphElementSchemas.getSchemaFor(label);

                PropertyGraphPrinter propertyGraphPrinter = writerFactory.createPrinter(label, index, labelSchema, targetConfig);
                LabelWriter<T> labelWriter = writerFactory.createLabelWriter(propertyGraphPrinter);

                labelWriters.put(label, labelWriter);
                fileSpecificLabelSchemas.add(labelWriter.outputId(), targetConfig.format(), labelSchema);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
