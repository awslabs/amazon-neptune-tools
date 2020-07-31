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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.propertygraph.ExportStats;
import com.amazonaws.services.neptune.propertygraph.Scope;
import com.amazonaws.services.neptune.propertygraph.metadata.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.TokensOnly;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PropertyGraphScopeModule {

    @Option(name = {"-nl", "--node-label"}, description = "Labels of nodes to be included in config (optional, default all labels)",
            arity = 1)
    private List<String> nodeLabels = new ArrayList<>();

    @Option(name = {"-el", "--edge-label"}, description = "Labels of edges to be included in config (optional, default all labels)",
            arity = 1)
    private List<String> edgeLabels = new ArrayList<>();

    @Option(name = {"-s", "--scope"}, description = "Scope (optional, default 'all')")
    @Once
    @AllowedValues(allowedValues = {"all", "nodes", "edges"})
    private Scope scope = Scope.all;

    @Option(name = {"--tokens-only"}, description = "Export tokens (~id, ~label) only (optional, default 'off')")
    @Once
    @AllowedValues(allowedValues = {"off", "nodes", "edges", "both"})
    private TokensOnly tokensOnly = TokensOnly.off;

    public Collection<ExportSpecification<?>> exportSpecifications(ExportStats stats, Collection<String> labModeFeatures){
        return scope.exportSpecifications(nodeLabels, edgeLabels, tokensOnly, stats, labModeFeatures);
    }
}
