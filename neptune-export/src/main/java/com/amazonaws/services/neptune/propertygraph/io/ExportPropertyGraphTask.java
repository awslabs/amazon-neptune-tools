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
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.schema.FileSpecificLabelSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class ExportPropertyGraphTask<T extends Map<?, ?>> implements Callable<FileSpecificLabelSchemas> {

    private static final Logger logger = LoggerFactory.getLogger(ExportPropertyGraphTask.class);

    private final GraphElementSchemas graphElementSchemas;
    private final LabelsFilter labelsFilter;
    private final GraphClient<T> graphClient;
    private final WriterFactory<T> writerFactory;
    private final PropertyGraphTargetConfig targetConfig;
    private final RangeFactory rangeFactory;
    private final GremlinFilters gremlinFilters;
    private final Status status;
    private final AtomicInteger index;
    private final LabelWriters<T> labelWriters;

    public ExportPropertyGraphTask(GraphElementSchemas graphElementSchemas,
                                   LabelsFilter labelsFilter,
                                   GraphClient<T> graphClient,
                                   WriterFactory<T> writerFactory,
                                   PropertyGraphTargetConfig targetConfig,
                                   RangeFactory rangeFactory,
                                   GremlinFilters gremlinFilters,
                                   Status status,
                                   AtomicInteger index,
                                   AtomicInteger fileDescriptorCount) {
        this.graphElementSchemas = graphElementSchemas;
        this.labelsFilter = labelsFilter;
        this.graphClient = graphClient;
        this.writerFactory = writerFactory;
        this.targetConfig = targetConfig;
        this.rangeFactory = rangeFactory;
        this.gremlinFilters = gremlinFilters;
        this.status = status;
        this.index = index;
        this.labelWriters = new LabelWriters<>(fileDescriptorCount);
    }

    @Override
    public FileSpecificLabelSchemas call() {

        FileSpecificLabelSchemas fileSpecificLabelSchemas = new FileSpecificLabelSchemas();

        CountingHandler handler = new CountingHandler(
                new TaskHandler(
                        fileSpecificLabelSchemas,
                        graphElementSchemas,
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
                    graphClient.queryForValues(handler, range, labelsFilter, gremlinFilters, graphElementSchemas);
                    if (range.sizeExceeds(handler.numberProcessed()) || rangeFactory.isExhausted()) {
                        status.halt();
                    }
                }
            }
        } finally {
            try {
                handler.close();
            } catch (Exception e) {
                logger.error("Error while closing handler", e);
            }
        }

        return fileSpecificLabelSchemas;
    }

    private class TaskHandler implements GraphElementHandler<T> {

        private final FileSpecificLabelSchemas fileSpecificLabelSchemas;
        private final GraphElementSchemas graphElementSchemas;
        private final PropertyGraphTargetConfig targetConfig;
        private final WriterFactory<T> writerFactory;
        private final LabelWriters<T> labelWriters;
        private final GraphClient<T> graphClient;
        private final Status status;
        private final AtomicInteger index;

        private TaskHandler(FileSpecificLabelSchemas fileSpecificLabelSchemas,
                            GraphElementSchemas graphElementSchemas,
                            PropertyGraphTargetConfig targetConfig,
                            WriterFactory<T> writerFactory,
                            LabelWriters<T> labelWriters,
                            GraphClient<T> graphClient,
                            Status status,
                            AtomicInteger index) {
            this.fileSpecificLabelSchemas = fileSpecificLabelSchemas;
            this.graphElementSchemas = graphElementSchemas;
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
            Label label = graphClient.getLabelFor(input, labelsFilter);
            if (!labelWriters.containsKey(label)) {
                createWriterFor(label);
            }
            graphClient.updateStats(label);
            labelWriters.get(label).handle(input, allowTokens);
        }

        @Override
        public void close() {
            try {
                labelWriters.close();
            } catch (Exception e) {
                logger.warn("Error closing label writer: {}.", e.getMessage());
            }
        }

        private void createWriterFor(Label label) {
            try {
                LabelSchema labelSchema = graphElementSchemas.getSchemaFor(label);

                PropertyGraphPrinter propertyGraphPrinter = writerFactory.createPrinter(
                        Directories.fileName(label.fullyQualifiedLabel(), index),
                        labelSchema,
                        targetConfig);
                LabelWriter<T> labelWriter = writerFactory.createLabelWriter(propertyGraphPrinter, labelSchema.label());

                labelWriters.put(label, labelWriter);
                fileSpecificLabelSchemas.add(labelWriter.outputId(), targetConfig.format(), labelSchema);

            } catch (Exception e) {
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
