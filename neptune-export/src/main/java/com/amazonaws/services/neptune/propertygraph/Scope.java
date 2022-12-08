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

package com.amazonaws.services.neptune.propertygraph;

import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.propertygraph.schema.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementType;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.propertygraph.schema.TokensOnly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public enum Scope {

    all {
        @Override
        public Collection<ExportSpecification> exportSpecifications(GraphSchema graphSchema,
                                                                    Collection<Label> nodeLabels,
                                                                    Collection<Label> edgeLabels,
                                                                    GremlinFilters gremlinFilters,
                                                                    TokensOnly tokensOnly,
                                                                    EdgeLabelStrategy edgeLabelStrategy,
                                                                    ExportStats stats,
                                                                    FeatureToggles featureToggles) {

            Collection<ExportSpecification> results = new ArrayList<>();

            if (graphSchema.isEmpty()) {
                results.add(new ExportSpecification(
                        GraphElementType.nodes,
                        Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly),
                        gremlinFilters,
                        stats,
                        tokensOnly.nodeTokensOnly(),
                        featureToggles));
                results.add(new ExportSpecification(
                        GraphElementType.edges,
                        Scope.labelsFilter(edgeLabels, edgeLabelStrategy),
                        gremlinFilters,
                        stats,
                        tokensOnly.edgeTokensOnly(),
                        featureToggles));
            } else {
                if (graphSchema.hasNodeSchemas()) {
                    LabelsFilter labelsFilter = Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly)
                            .intersection(graphSchema.graphElementSchemasFor(GraphElementType.nodes).labels());
                    if (!labelsFilter.isEmpty()) {
                        results.add(new ExportSpecification(
                                GraphElementType.nodes,
                                labelsFilter,
                                gremlinFilters, stats,
                                tokensOnly.nodeTokensOnly(),
                                featureToggles));
                    }
                }
                if (graphSchema.hasEdgeSchemas()) {
                    LabelsFilter labelsFilter = Scope.labelsFilter(edgeLabels, edgeLabelStrategy)
                            .intersection(graphSchema.graphElementSchemasFor(GraphElementType.edges).labels());
                    if (!labelsFilter.isEmpty()) {
                        results.add(new ExportSpecification(
                                GraphElementType.edges,
                                labelsFilter,
                                gremlinFilters, stats,
                                tokensOnly.edgeTokensOnly(),
                                featureToggles));
                    }
                }
            }

            return results;
        }
    },
    nodes {
        @Override
        public Collection<ExportSpecification> exportSpecifications(GraphSchema graphSchema,
                                                                    Collection<Label> nodeLabels,
                                                                    Collection<Label> edgeLabels,
                                                                    GremlinFilters gremlinFilters,
                                                                    TokensOnly tokensOnly,
                                                                    EdgeLabelStrategy edgeLabelStrategy,
                                                                    ExportStats stats,
                                                                    FeatureToggles featureToggles) {
            if (graphSchema.isEmpty()) {
                return Collections.singletonList(
                        new ExportSpecification(
                                GraphElementType.nodes,
                                Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly),
                                gremlinFilters, stats, tokensOnly.nodeTokensOnly(),
                                featureToggles)
                );
            } else if (graphSchema.hasNodeSchemas()) {
                LabelsFilter labelsFilter = Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly)
                        .intersection(graphSchema.graphElementSchemasFor(GraphElementType.nodes).labels());
                if (!labelsFilter.isEmpty()) {
                    return Collections.singletonList(
                            new ExportSpecification(
                                    GraphElementType.nodes,
                                    labelsFilter,
                                    gremlinFilters,
                                    stats,
                                    tokensOnly.nodeTokensOnly(),
                                    featureToggles)
                    );
                } else {
                    return Collections.emptyList();
                }

            } else {
                return Collections.emptyList();
            }

        }
    },
    edges {
        @Override
        public Collection<ExportSpecification> exportSpecifications(GraphSchema graphSchema,
                                                                    Collection<Label> nodeLabels,
                                                                    Collection<Label> edgeLabels,
                                                                    GremlinFilters gremlinFilters,
                                                                    TokensOnly tokensOnly,
                                                                    EdgeLabelStrategy edgeLabelStrategy,
                                                                    ExportStats stats,
                                                                    FeatureToggles featureToggles) {
            if (graphSchema.isEmpty()) {
                return Collections.singletonList(
                        new ExportSpecification(
                                GraphElementType.edges,
                                Scope.labelsFilter(edgeLabels, edgeLabelStrategy),
                                gremlinFilters,
                                stats,
                                tokensOnly.edgeTokensOnly(),
                                featureToggles)
                );
            } else if (graphSchema.hasEdgeSchemas()) {
                LabelsFilter labelsFilter = Scope.labelsFilter(edgeLabels, edgeLabelStrategy)
                        .intersection(graphSchema.graphElementSchemasFor(GraphElementType.edges).labels());
                if (!labelsFilter.isEmpty()) {
                    return Collections.singletonList(
                            new ExportSpecification(
                                    GraphElementType.edges,
                                    labelsFilter,
                                    gremlinFilters,
                                    stats,
                                    tokensOnly.edgeTokensOnly(),
                                    featureToggles)
                    );
                } else {
                    return Collections.emptyList();
                }
            } else {
                return Collections.emptyList();
            }
        }
    };

    private static LabelsFilter labelsFilter(Collection<Label> labels, LabelStrategy labelStrategy) {
        if (labels.isEmpty()) {
            return new AllLabels(labelStrategy);
        }

        return new SpecifiedLabels(labels, labelStrategy);
    }

    public abstract Collection<ExportSpecification> exportSpecifications(
            GraphSchema graphSchema,
            Collection<Label> nodeLabels,
            Collection<Label> edgeLabels,
            GremlinFilters gremlinFilters,
            TokensOnly tokensOnly,
            EdgeLabelStrategy edgeLabelStrategy,
            ExportStats stats,
            FeatureToggles featureToggles);

}
