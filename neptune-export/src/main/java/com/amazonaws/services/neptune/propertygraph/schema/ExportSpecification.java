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
import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.io.StatusOutputFormat;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.io.ExportPropertyGraphTask;
import com.amazonaws.services.neptune.propertygraph.io.GraphElementHandler;
import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphTargetConfig;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExportSpecification {
    private final GraphElementType graphElementType;
    private final LabelsFilter labelsFilter;
    private final GremlinFilters gremlinFilters;
    private final boolean tokensOnly;
    private final ExportStats stats;
    private final FeatureToggles featureToggles;


    public ExportSpecification(GraphElementType graphElementType,
                               LabelsFilter labelsFilter,
                               GremlinFilters gremlinFilters,
                               ExportStats stats,
                               boolean tokensOnly,
                               FeatureToggles featureToggles) {
        this.graphElementType = graphElementType;
        this.labelsFilter = labelsFilter;
        this.gremlinFilters = gremlinFilters;
        this.tokensOnly = tokensOnly;
        this.stats = stats;
        this.featureToggles = featureToggles;
    }

    public void scan(GraphSchema graphSchema, GraphTraversalSource g) {
        if (tokensOnly) {
            return;
        }

        GraphClient<Map<String, Object>> graphClient = graphElementType.graphClient(g, tokensOnly, stats, featureToggles);

        graphClient.queryForSchema(
                new CreateSchemaHandler(graphElementType, graphSchema),
                Range.ALL,
                labelsFilter,
                gremlinFilters);
    }

    public void sample(GraphSchema graphSchema, GraphTraversalSource g, long sampleSize) {
        if (tokensOnly) {
            return;
        }

        GraphClient<Map<String, Object>> graphClient = graphElementType.graphClient(g, tokensOnly, stats, featureToggles);
        Collection<Label> labels = labelsFilter.getLabelsUsing(graphClient);

        for (Label label : labels) {
            graphClient.queryForSchema(
                    new CreateSchemaHandler(graphElementType, graphSchema),
                    new Range(0, sampleSize),
                    labelsFilter.filterFor(label),
                    gremlinFilters);
        }
    }

    public String description() {
        return labelsFilter.description(graphElementType.name());
    }

    public RangeFactory createRangeFactory(GraphTraversalSource g,
                                           RangeConfig rangeConfig,
                                           ConcurrencyConfig concurrencyConfig) {
        return RangeFactory.create(
                graphElementType.graphClient(g, tokensOnly, stats, featureToggles),
                labelsFilter,
                gremlinFilters, rangeConfig,
                concurrencyConfig);
    }

    public ExportPropertyGraphTask<Map<String, Object>> createExportTask(GraphSchema graphSchema,
                                                                         GraphTraversalSource g,
                                                                         PropertyGraphTargetConfig targetConfig,
                                                                         GremlinFilters gremlinFilters,
                                                                         RangeFactory rangeFactory,
                                                                         Status status,
                                                                         AtomicInteger index,
                                                                         AtomicInteger fileDescriptorCount) {
        return new ExportPropertyGraphTask<>(
                graphSchema.copyOfGraphElementSchemasFor(graphElementType),
                labelsFilter,
                graphElementType.graphClient(g, tokensOnly, stats, featureToggles),
                graphElementType.writerFactory(),
                targetConfig,
                rangeFactory,
                gremlinFilters,
                status,
                index,
                fileDescriptorCount
        );
    }

    public MasterLabelSchemas createMasterLabelSchemas(Collection<FileSpecificLabelSchemas> fileSpecificLabelSchemasCollection) {

        Set<Label> labels = new HashSet<>();

        fileSpecificLabelSchemasCollection.forEach(s -> labels.addAll(s.labels()));

        Map<Label, MasterLabelSchema> masterLabelSchemas = new HashMap<>();

        for (Label label : labels) {

            LabelSchema masterLabelSchema = new LabelSchema(label);
            Collection<FileSpecificLabelSchema> fileSpecificLabelSchemas = new ArrayList<>();

            for (FileSpecificLabelSchemas fileSpecificLabelSchemasForTask : fileSpecificLabelSchemasCollection) {
                if (fileSpecificLabelSchemasForTask.hasSchemasForLabel(label)) {
                    for (FileSpecificLabelSchema fileSpecificLabelSchema :
                            fileSpecificLabelSchemasForTask.fileSpecificLabelSchemasFor(label)) {
                        masterLabelSchema = masterLabelSchema.union(fileSpecificLabelSchema.labelSchema());
                        fileSpecificLabelSchemas.add(fileSpecificLabelSchema);
                    }
                }
            }

            masterLabelSchemas.put(
                    label,
                    new MasterLabelSchema(masterLabelSchema, fileSpecificLabelSchemas));


        }

        return new MasterLabelSchemas(masterLabelSchemas, graphElementType);
    }

    public Collection<ExportSpecification> splitByLabel() {

        if (graphElementType == GraphElementType.edges || featureToggles.containsFeature(FeatureToggle.ExportByIndividualLabels)) {
            return labelsFilter.split().stream()
                    .map(l -> new ExportSpecification(graphElementType, l, gremlinFilters, stats, tokensOnly, featureToggles))
                    .collect(Collectors.toList());

        } else {
            return Collections.singletonList(this);
        }
    }

    private static class CreateSchemaHandler implements GraphElementHandler<Map<?, Object>> {

        private final GraphElementType graphElementType;
        private final GraphSchema graphSchema;
        private final Status status;

        private CreateSchemaHandler(GraphElementType graphElementType, GraphSchema graphSchema) {
            this.graphElementType = graphElementType;
            this.graphSchema = graphSchema;
            this.status = new Status(StatusOutputFormat.Dot);
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
