/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.schema.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.amazonaws.services.neptune.propertygraph.schema.TokensOnly;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.Once;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PropertyGraphScopeModule {

    @Option(name = {"-nl", "--node-label"}, description = "Labels of nodes to be included in config (optional, default all labels).",
            arity = 1)
    private List<String> nodeLabels = new ArrayList<>();

    @Option(name = {"-el", "--edge-label"}, description = "Labels of edges to be included in config (optional, default all labels).",
            arity = 1)
    private List<String> edgeLabels = new ArrayList<>();

    @Option(name = {"-s", "--scope"}, description = "Scope (optional, default 'all').")
    @Once
    @AllowedEnumValues(Scope.class)
    private Scope scope = Scope.all;

    @Option(name = {"--tokens-only"}, description = "Export tokens (~id, ~label, ~from, ~to) only (optional, default 'off').")
    @Once
    @AllowedEnumValues(TokensOnly.class)
    private TokensOnly tokensOnly = TokensOnly.off;

    @Option(name = {"--edge-label-strategy"}, description = "Export edges by their edge labels, or by a combination of their start vertex label, edge label, and end vertex label (optional, default 'edgeLabelsOnly').")
    @Once
    @AllowedEnumValues(EdgeLabelStrategy.class)
    private EdgeLabelStrategy edgeLabelStrategy = EdgeLabelStrategy.edgeLabelsOnly;

    public Collection<ExportSpecification> exportSpecifications(ExportStats stats,
                                                                GremlinFilters gremlinFilters,
                                                                FeatureToggles featureToggles){
        return exportSpecifications(new GraphSchema(), gremlinFilters, stats, featureToggles);
    }

    public Collection<ExportSpecification> exportSpecifications(GraphSchema graphSchema,
                                                                GremlinFilters gremlinFilters,
                                                                ExportStats stats,
                                                                FeatureToggles featureToggles){
        return scope.exportSpecifications(
                graphSchema,
                Label.forLabels(nodeLabels),
                Label.forLabels(edgeLabels),
                gremlinFilters,
                tokensOnly,
                edgeLabelStrategy,
                stats,
                featureToggles);
    }
}
