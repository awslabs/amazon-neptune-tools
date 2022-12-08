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

import com.amazonaws.services.neptune.propertygraph.GremlinFilters;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class GremlinFiltersModule {
    @Option(name = {"--gremlin-node-filter"}, description = "Gremlin steps for filtering nodes (overrides --gremlin-filter).")
    @Once
    private String gremlinNodeFilter;

    @Option(name = {"--gremlin-edge-filter"}, description = "Gremlin steps for filtering edges (overrides --gremlin-filter).")
    @Once
    private String gremlinEdgeFilter;

    @Option(name = {"--gremlin-filter"}, description = "Gremlin steps for filtering nodes and edges.")
    @Once
    private String gremlinFilter;

    public GremlinFilters filters(){
        return new GremlinFilters(gremlinFilter, gremlinNodeFilter, gremlinEdgeFilter);
    }
}
