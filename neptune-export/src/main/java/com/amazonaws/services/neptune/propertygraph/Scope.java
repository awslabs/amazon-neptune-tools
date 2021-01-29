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

import com.amazonaws.services.neptune.export.LabModeFeatures;
import com.amazonaws.services.neptune.propertygraph.schema.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementTypes;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.propertygraph.schema.TokensOnly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public enum Scope {

    all {
        @Override
        public Collection<ExportSpecification<?>> exportSpecifications(GraphSchema graphSchema,
                                                                       Collection<Label> nodeLabels,
                                                                       Collection<Label> edgeLabels,
                                                                       TokensOnly tokensOnly,
                                                                       EdgeLabelStrategy edgeLabelStrategy,
                                                                       ExportStats stats,
                                                                       LabModeFeatures labModeFeatures) {

            Collection<ExportSpecification<?>> results = new ArrayList<>();

            if (graphSchema.isEmpty()) {
                results.add(new ExportSpecification<>(
                        GraphElementTypes.Nodes,
                        Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly),
                        stats, tokensOnly.nodeTokensOnly(),
                        labModeFeatures));
                results.add(new ExportSpecification<>(
                        GraphElementTypes.Edges,
                        Scope.labelsFilter(edgeLabels, edgeLabelStrategy),
                        stats, tokensOnly.edgeTokensOnly(),
                        labModeFeatures));
            } else {
                if (graphSchema.hasNodeSchemas()) {
                    LabelsFilter labelsFilter = Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly)
                            .union(graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes).labels());
                    if (!labelsFilter.isEmpty()) {
                        results.add(new ExportSpecification<>(
                                GraphElementTypes.Nodes,
                                labelsFilter,
                                stats, tokensOnly.nodeTokensOnly(),
                                labModeFeatures));
                    }
                }
                if (graphSchema.hasEdgeSchemas()) {
                    LabelsFilter labelsFilter = Scope.labelsFilter(edgeLabels, edgeLabelStrategy)
                            .union(graphSchema.graphElementSchemasFor(GraphElementTypes.Edges).labels());
                    if (!labelsFilter.isEmpty()) {
                        results.add(new ExportSpecification<>(
                                GraphElementTypes.Edges,
                                labelsFilter,
                                stats, tokensOnly.edgeTokensOnly(),
                                labModeFeatures));
                    }
                }
            }

            return results;
        }
    },
    nodes {
        @Override
        public Collection<ExportSpecification<?>> exportSpecifications(GraphSchema graphSchema,
                                                                       Collection<Label> nodeLabels,
                                                                       Collection<Label> edgeLabels,
                                                                       TokensOnly tokensOnly,
                                                                       EdgeLabelStrategy edgeLabelStrategy,
                                                                       ExportStats stats,
                                                                       LabModeFeatures labModeFeatures) {
            if (graphSchema.isEmpty()) {
                return Collections.singletonList(
                        new ExportSpecification<>(
                                GraphElementTypes.Nodes,
                                Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly),
                                stats, tokensOnly.nodeTokensOnly(),
                                labModeFeatures)
                );
            } else if (graphSchema.hasNodeSchemas()) {
                LabelsFilter labelsFilter = Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly)
                        .union(graphSchema.graphElementSchemasFor(GraphElementTypes.Nodes).labels());
                if (!labelsFilter.isEmpty()) {
                    return Collections.singletonList(
                            new ExportSpecification<>(
                                    GraphElementTypes.Nodes,
                                    labelsFilter,
                                    stats, tokensOnly.nodeTokensOnly(),
                                    labModeFeatures)
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
        public Collection<ExportSpecification<?>> exportSpecifications(GraphSchema graphSchema,
                                                                       Collection<Label> nodeLabels,
                                                                       Collection<Label> edgeLabels,
                                                                       TokensOnly tokensOnly,
                                                                       EdgeLabelStrategy edgeLabelStrategy,
                                                                       ExportStats stats,
                                                                       LabModeFeatures labModeFeatures) {
            if (graphSchema.isEmpty()) {
                return Collections.singletonList(
                        new ExportSpecification<>(
                                GraphElementTypes.Edges,
                                Scope.labelsFilter(edgeLabels, edgeLabelStrategy),
                                stats, tokensOnly.edgeTokensOnly(),
                                labModeFeatures)
                );
            } else if (graphSchema.hasEdgeSchemas()) {
                LabelsFilter labelsFilter = Scope.labelsFilter(edgeLabels, edgeLabelStrategy)
                        .union(graphSchema.graphElementSchemasFor(GraphElementTypes.Edges).labels());
                if (!labelsFilter.isEmpty()) {
                    return Collections.singletonList(
                            new ExportSpecification<>(
                                    GraphElementTypes.Edges,
                                    labelsFilter,
                                    stats, tokensOnly.edgeTokensOnly(),
                                    labModeFeatures)
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

    public abstract Collection<ExportSpecification<?>> exportSpecifications(
            GraphSchema graphSchema,
            Collection<Label> nodeLabels,
            Collection<Label> edgeLabels,
            TokensOnly tokensOnly,
            EdgeLabelStrategy edgeLabelStrategy,
            ExportStats stats,
            LabModeFeatures labModeFeatures);

}
