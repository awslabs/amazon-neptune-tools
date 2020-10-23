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
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.io.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.Map;

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

    public void rewrite(MasterLabelSchemas masterLabelSchemas, PropertyGraphTargetConfig targetConfig) throws Exception {
        RewriteCommand rewriteCommand = targetConfig.createRewriteCommand();
        rewriteCommand.execute(masterLabelSchemas, targetConfig, graphElementType);
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
