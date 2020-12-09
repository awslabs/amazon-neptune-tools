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
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.*;

public class AllLabels implements LabelsFilter {

    private final LabelStrategy labelStrategy;

    public AllLabels(LabelStrategy labelStrategy) {
        this.labelStrategy = labelStrategy;
    }

    @Override
    public GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal, LabModeFeatures labModeFeatures) {
        return traversal;
    }

    @Override
    public Collection<Label> getLabelsUsing(GraphClient<?> graphClient) {
        return graphClient.labels(labelStrategy);
    }

    @Override
    public String[] getPropertiesForLabels(GraphElementSchemas graphElementSchemas) {

        Set<String> properties = new HashSet<>();

        Iterable<Label> labels = graphElementSchemas.labels();

        for (Label label : labels) {
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(label);
            for (PropertySchema propertySchema : labelSchema.propertySchemas()) {
                properties.add(propertySchema.nameWithoutDataType());
            }
        }

        return properties.toArray(new String[]{});
    }

    @Override
    public Label getLabelFor(Map<String, Object> input) {
        return labelStrategy.getLabelFor(input);
    }

    @Override
    public String[] addAdditionalColumnNames(String... columns) {
        return labelStrategy.additionalColumns(columns);
    }

    @Override
    public <T> GraphTraversal<? extends Element, T> addAdditionalColumns(GraphTraversal<? extends Element, T> t) {
        return labelStrategy.addAdditionalColumns(t);
    }

    @Override
    public LabelsFilter filterFor(Label label) {
        return new SpecifiedLabels(Collections.singletonList(label), labelStrategy);
    }

    @Override
    public LabelsFilter union(Collection<Label> labels) {
        return new SpecifiedLabels(labels, labelStrategy);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public String description(String element) {
        return String.format("all %s", element);
    }

    @Override
    public Collection<LabelsFilter> split() {
        return Collections.singletonList(this);
    }
}
