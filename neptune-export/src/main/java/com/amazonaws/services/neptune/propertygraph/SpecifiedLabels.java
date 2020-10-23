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

import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.*;
import java.util.stream.Collectors;

public class SpecifiedLabels implements LabelsFilter {

    public static LabelsFilter forLabels(String... labels){
        return new SpecifiedLabels(Label.forLabels(labels));
    }

    private final Collection<Label> labels;

    public SpecifiedLabels(Collection<Label> labels) {
        this.labels = labels;
    }

    @Override
    public GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal) {
        Label firstLabel = labels.stream().findFirst().orElseThrow(()->new IllegalStateException("No labels specified"));
        String[] remainingLabels = labels.stream()
                .skip(1)
                .map(Label::label)
                .collect(Collectors.toList())
                .toArray(new String[]{});

        return traversal.hasLabel(firstLabel.label(), remainingLabels);
    }

    @Override
    public Collection<Label> resolveLabels(GraphClient<?> graphClient) {
        return labels;
    }

    @Override
    public String[] getPropertiesForLabels(GraphElementSchemas graphElementSchemas) {
        Set<String> properties = new HashSet<>();

        for (Label label : labels) {
            LabelSchema labelSchema = graphElementSchemas.getSchemaFor(label.fullyQualifiedLabel());
            for (PropertySchema propertySchema : labelSchema.propertySchemas()) {
                properties.add(propertySchema.nameWithoutDataType());
            }
        }

        return properties.toArray(new String[]{});
    }
}
