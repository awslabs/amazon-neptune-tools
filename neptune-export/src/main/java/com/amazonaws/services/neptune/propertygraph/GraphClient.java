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

import com.amazonaws.services.neptune.propertygraph.io.GraphElementHandler;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;

import java.util.Collection;
import java.util.Map;

public interface GraphClient<T> {
    String description();

    void queryForSchema(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter, GremlinFilters gremlinFilters);

    void queryForValues(GraphElementHandler<T> handler, Range range, LabelsFilter labelsFilter, GremlinFilters gremlinFilters, GraphElementSchemas graphElementSchemas);

    long approxCount(LabelsFilter labelsFilter, RangeConfig rangeConfig, GremlinFilters gremlinFilters);

    Collection<Label> labels(LabelStrategy labelStrategy);

    Label getLabelFor(T input, LabelsFilter labelsFilter);

    void updateStats(Label label);
}
