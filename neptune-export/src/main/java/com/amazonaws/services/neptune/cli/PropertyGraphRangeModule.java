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

import com.amazonaws.services.neptune.propertygraph.RangeConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class PropertyGraphRangeModule {

    @Option(name = {"-r", "--range", "--range-size"}, description = "Number of items to fetch per request (optional)")
    @Once
    private long rangeSize = -1;

    @Option(name = {"--limit"}, description = "Maximum number of items to export (optional)")
    @Once
    private long limit = Long.MAX_VALUE;

    @Option(name = {"--skip"}, description = "Number of items to skip (optional)")
    @Once
    private long skip = 0;

    @Option(name = {"--approx-node-count"}, description = "Approximate number of nodes in graph")
    @Once
    private long approxNodeCount = -1;

    @Option(name = {"--approx-edge-count"}, description = "Approximate number of edges in graph")
    @Once
    private long approxEdgeCount = -1;

    public RangeConfig config(){
        return new RangeConfig(rangeSize, skip, limit, approxNodeCount, approxEdgeCount);
    }
}
