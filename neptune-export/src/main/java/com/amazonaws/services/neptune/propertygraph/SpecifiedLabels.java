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

import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.GraphElementType;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class SpecifiedLabels implements LabelsFilter {

    private final Collection<Label> labels;
    private final LabelStrategy labelStrategy;

    public SpecifiedLabels(Collection<Label> labels, LabelStrategy labelStrategy) {
        this.labels = labels;
        this.labelStrategy = labelStrategy;
    }

    @Override
    public GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal, FeatureToggles featureToggles, GraphElementType graphElementType) {

        boolean simpleEdgeLabels = graphElementType == GraphElementType.edges &&
                labels.stream().allMatch(l -> !l.hasFromLabels() && !l.hasToLabels());

        if (simpleEdgeLabels || featureToggles.containsFeature(FeatureToggle.ExportByIndividualLabels)) {

            List<String> labelList = labels.stream()
                    .flatMap((Function<Label, Stream<String>>) label -> label.labels().stream())
                    .collect(Collectors.toList());

            String firstLabel = labelList.stream().findFirst().orElseThrow(() -> new IllegalStateException("No labels specified"));
            String[] remainingLabels = labelList.stream()
                    .skip(1)
                    .collect(Collectors.toList())
                    .toArray(new String[]{});

            return traversal.hasLabel(firstLabel, remainingLabels);
        } else {

            if (labels.size() > 1) {

                List<Traversal<?, ?>> traversals = new ArrayList<>();
                for (Label label : labels) {
                    traversals.add(createFilterForLabel(label, null));
                }
                traversal = traversal.or(traversals.toArray(new Traversal<?, ?>[]{}));


            } else {

                Label label = labels.iterator().next();
                traversal = createFilterForLabel(label, traversal);

            }

            return traversal;
        }
    }

    private GraphTraversal<? extends Element, ?> createFilterForLabel(Label label, GraphTraversal<? extends Element, ?> t) {


        for (String labelValue : label.labels()) {
            if (t == null) {
                t = hasLabel(labelValue);
            } else {
                t = t.hasLabel(labelValue);
            }
        }

        if (labelStrategy == EdgeLabelStrategy.edgeAndVertexLabels) {
            if (label.hasFromAndToLabels()) {
                List<Traversal<?, ?>> traversals = new ArrayList<>();

                GraphTraversal<? extends Element, ?> startVertex = outV();
                startVertex = createFilterForLabel(label.fromLabels(), startVertex);
                traversals.add(startVertex);

                GraphTraversal<? extends Element, ?> endVertex = inV();
                endVertex = createFilterForLabel(label.toLabels(), endVertex);
                traversals.add(endVertex);

                t = t.where(and(traversals.toArray(new Traversal<?, ?>[]{})));
            } else if (label.hasFromLabels()) {
                GraphTraversal<? extends Element, ?> startVertex = outV();
                startVertex = createFilterForLabel(label.fromLabels(), startVertex);
                t = t.where(startVertex);
            } else if (label.hasToLabels()) {
                GraphTraversal<? extends Element, ?> endVertex = inV();
                endVertex = createFilterForLabel(label.toLabels(), endVertex);
                t = t.where(endVertex);
            }
        }


        return t;
    }

    @Override
    public Collection<Label> getLabelsUsing(GraphClient<?> graphClient) {
        return labels;
    }

    @Override
    public String[] getPropertiesForLabels(GraphElementSchemas graphElementSchemas) {
        Set<String> properties = new HashSet<>();

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
    public LabelsFilter intersection(Collection<Label> others) {
        Collection<Label> results = new HashSet<>();
        for (Label label : labels) {
            for (Label other : others) {
                if (label.isAssignableFrom(other)){
                    results.add(other);
                }
            }
        }
        return new SpecifiedLabels(results, labelStrategy);
    }

    @Override
    public boolean isEmpty() {
        return labels.isEmpty();
    }

    @Override
    public String description(String element) {
        if (isEmpty()){
            return String.format("%s with zero labels", element);
        }
        String labelList = labels.stream().map(l -> String.format("'%s'", l.fullyQualifiedLabel())).collect(Collectors.joining(" or "));
        return String.format("%s with label(s) %s", element, labelList);
    }

    @Override
    public Collection<LabelsFilter> split() {
        return labels.stream()
                .map(l -> new SpecifiedLabels(Collections.singletonList(l), labelStrategy))
                .collect(Collectors.toList());
    }
}
