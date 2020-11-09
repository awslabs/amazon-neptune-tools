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

import com.amazonaws.services.neptune.propertygraph.schema.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementTypes;
import com.amazonaws.services.neptune.propertygraph.schema.TokensOnly;

import java.util.*;
import java.util.stream.Collectors;

public enum Scope {

    all {
        @Override
        public Collection<ExportSpecification<?>> exportSpecifications(Collection<Label> nodeLabels,
                                                                       Collection<Label> edgeLabels,
                                                                       TokensOnly tokensOnly,
                                                                       EdgeLabelStrategy edgeLabelStrategy,
                                                                       ExportStats stats,
                                                                       Collection<String> labModeFeatures) {
            return Arrays.asList(
                    new ExportSpecification<>(
                            GraphElementTypes.Nodes,
                            Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly),
                            tokensOnly.nodeTokensOnly(),
                            stats,
                            labModeFeatures),
                    new ExportSpecification<>(
                            GraphElementTypes.Edges,
                            Scope.labelsFilter(edgeLabels, edgeLabelStrategy),
                            tokensOnly.edgeTokensOnly(),
                            stats,
                            labModeFeatures)
            );
        }
    },
    nodes {
        @Override
        public Collection<ExportSpecification<?>> exportSpecifications(Collection<Label> nodeLabels,
                                                                       Collection<Label> edgeLabels,
                                                                       TokensOnly tokensOnly,
                                                                       EdgeLabelStrategy edgeLabelStrategy,
                                                                       ExportStats stats,
                                                                       Collection<String> labModeFeatures) {
            return Collections.singletonList(
                    new ExportSpecification<>(
                            GraphElementTypes.Nodes,
                            Scope.labelsFilter(nodeLabels, NodeLabelStrategy.nodeLabelsOnly),
                            tokensOnly.nodeTokensOnly(),
                            stats,
                            labModeFeatures)
            );
        }
    },
    edges {
        @Override
        public Collection<ExportSpecification<?>> exportSpecifications(Collection<Label> nodeLabels,
                                                                       Collection<Label> edgeLabels,
                                                                       TokensOnly tokensOnly,
                                                                       EdgeLabelStrategy edgeLabelStrategy,
                                                                       ExportStats stats,
                                                                       Collection<String> labModeFeatures) {
            return Collections.singletonList(
                    new ExportSpecification<>(
                            GraphElementTypes.Edges,
                            Scope.labelsFilter(edgeLabels, edgeLabelStrategy),
                            tokensOnly.edgeTokensOnly(),
                            stats,
                            labModeFeatures)
            );
        }
    };

    private static Set<String> toSet(Collection<String> labels) {
        return labels.stream().flatMap(v -> Arrays.stream(v.split(","))).collect(Collectors.toSet());
    }

    private static LabelsFilter labelsFilter(Collection<Label> labels, LabelStrategy labelStrategy){
        if (labels.isEmpty()){
            return new AllLabels(labelStrategy);
        }

        return new SpecifiedLabels(labels, labelStrategy);
    }

    public abstract Collection<ExportSpecification<?>> exportSpecifications(
            Collection<Label> nodeLabels,
            Collection<Label> edgeLabels,
            TokensOnly tokensOnly,
            EdgeLabelStrategy edgeLabelStrategy,
            ExportStats stats,
            Collection<String> labModeFeatures);

}
