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

package com.amazonaws.services.neptune.propertygraph.schema;

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.io.PrintOutputWriter;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.io.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ExportSpecification<T extends Map<?, ?>> {
    private final GraphElementType<T> graphElementType;
    private final LabelsFilter labelsFilter;
    private final boolean tokensOnly;
    private final ExportStats stats;
    private final Collection<String> labModeFeatures;

    public ExportSpecification(GraphElementType<T> graphElementType,
                               LabelsFilter labelsFilter,
                               boolean tokensOnly,
                               ExportStats stats,
                               Collection<String> labModeFeatures) {
        this.graphElementType = graphElementType;
        this.labelsFilter = labelsFilter;
        this.tokensOnly = tokensOnly;
        this.stats = stats;
        this.labModeFeatures = labModeFeatures;
    }

    public void scan(GraphSchema graphSchema, GraphTraversalSource g) {
        if (tokensOnly) {
            return;
        }

        GraphClient<T> graphClient = graphElementType.graphClient(g, tokensOnly, stats, labModeFeatures);

        graphClient.queryForSchema(
                new CreateSchemaHandler(graphElementType, graphSchema),
                Range.ALL,
                labelsFilter);
    }

    public void sample(GraphSchema graphSchema, GraphTraversalSource g, long sampleSize) {
        if (tokensOnly) {
            return;
        }

        GraphClient<T> graphClient = graphElementType.graphClient(g, tokensOnly, stats, labModeFeatures);
        Collection<String> labels = labelsFilter.resolveLabels(graphClient);

        for (String label : labels) {
            graphClient.queryForSchema(
                    new CreateSchemaHandler(graphElementType, graphSchema),
                    new Range(0, sampleSize),
                    SpecifiedLabels.forLabels(label));
        }
    }

    public String description() {
        return graphElementType.name();
    }

    public RangeFactory createRangeFactory(GraphTraversalSource g,
                                           RangeConfig rangeConfig,
                                           ConcurrencyConfig concurrencyConfig) {
        return RangeFactory.create(
                graphElementType.graphClient(g, tokensOnly, stats, labModeFeatures),
                labelsFilter,
                rangeConfig,
                concurrencyConfig);
    }

    public ExportPropertyGraphTask<T> createExportTask(GraphSchema graphSchema,
                                                       GraphTraversalSource g,
                                                       PropertyGraphTargetConfig targetConfig,
                                                       RangeFactory rangeFactory,
                                                       Status status,
                                                       int index) {
        return new ExportPropertyGraphTask<>(
                graphSchema,
                graphElementType,
                labelsFilter,
                graphElementType.graphClient(g, tokensOnly, stats, labModeFeatures),
                graphElementType.writerFactory(),
                targetConfig,
                rangeFactory,
                status,
                index
        );
    }

    public void updateGraphSchema(GraphSchema graphSchema, MasterLabelSchemas masterLabelSchemas) {
        masterLabelSchemas.updateGraphSchema(graphSchema, graphElementType);
    }

    public void rewrite(MasterLabelSchema masterLabelSchema, PropertyGraphTargetConfig targetConfig) throws Exception {

        LabelSchema masterSchema = masterLabelSchema.labelSchema();

        for (FileSpecificLabelSchema fileSpecificLabelSchema : masterLabelSchema.fileSpecificLabelSchemas()) {

            File file  = new File(fileSpecificLabelSchema.outputId());

            // need to add node or vertex token headers
            String[] filePropertyHeaders =
                    fileSpecificLabelSchema.labelSchema().propertySchemas().stream()
                    .map(p -> p.property().toString())
                    .collect(Collectors.toList())
                    .toArray(new String[]{});

            String[] fileHeaders = graphElementType.equals(GraphElementTypes.Nodes) ?
                    ArrayUtils.addAll(new String[]{"~id", "~label"}, filePropertyHeaders):
                    ArrayUtils.addAll(new String[]{"~id", "~label", "~from", "~to"}, filePropertyHeaders);

            Reader in = new FileReader(file);

            PropertyGraphPrinter printer = graphElementType.writerFactory().createPrinter(file.getName(), masterSchema, targetConfig, true);

            CSVFormat format = CSVFormat.RFC4180.withHeader(fileHeaders);
            Iterable<CSVRecord> records = format.parse(in);

            for (CSVRecord record : records) {
                printer.printStartRow();

                if (graphElementType.equals(GraphElementTypes.Nodes)){
                    printer.printNode(record.get("~id"), Arrays.asList(record.get("~label").split(";")));
                } else {
                    printer.printEdge(record.get("~id"), record.get("~label"), record.get("~from"), record.get("~to"));
                }

                printer.printProperties(record.toMap(), false);
                printer.printEndRow();
            }

            printer.close();

            file.delete();
            new File(printer.outputId()).renameTo(file);
        }
    }

    private static class CreateSchemaHandler implements GraphElementHandler<Map<?, Object>> {

        private final GraphElementType<?> graphElementType;
        private final GraphSchema graphSchema;
        private final Status status = new Status();

        private CreateSchemaHandler(GraphElementType<?> graphElementType, GraphSchema graphSchema) {
            this.graphElementType = graphElementType;
            this.graphSchema = graphSchema;
        }

        @Override
        public void handle(Map<?, Object> properties, boolean allowTokens) {
            status.update();
            graphSchema.update(graphElementType, properties, allowTokens);
        }

        @Override
        public void close() throws Exception {
            // Do nothing
        }
    }
}
