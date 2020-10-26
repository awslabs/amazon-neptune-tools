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

package com.amazonaws.services.neptune.propertygraph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class Label {

    public static Label fromJson(JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            ArrayNode startLabels = (ArrayNode) jsonNode.path("fromLabels");
            String label = jsonNode.path("name").textValue();
            ArrayNode endLabels = (ArrayNode) jsonNode.path("toLabels");
            Collection<String> fromLabels = new ArrayList<>();
            startLabels.forEach(l -> fromLabels.add(l.textValue()));
            Collection<String> toLabels = new ArrayList<>();
            endLabels.forEach(l -> toLabels.add(l.textValue()));
            return new Label(label, fromLabels, toLabels);
        } else {
            return new Label(jsonNode.textValue());
        }
    }

    public static Collection<Label> forLabels(Collection<String> labels) {
        Set<Label> results = new HashSet<>();
        for (String label : labels) {
            results.add(new Label(label));
        }
        return results;
    }

    private final String label;
    private final String fullyQualifiedLabel;
    private final Collection<String> fromLabels;
    private final Collection<String> toLabels;

    public Label(String label) {
        this(label, Collections.emptyList(), Collections.emptyList());
    }

    public Label(String label, Collection<String> fromLabels, Collection<String> toLabels) {
        this.label = label;
        this.fromLabels = fromLabels;
        this.toLabels = toLabels;
        this.fullyQualifiedLabel = fromLabels.isEmpty() || toLabels.isEmpty() ?
                label :
                String.format("(%s)-[%s]->(%s)", String.join("|", fromLabels), label, String.join("|", toLabels));
    }

    public String label() {
        return label;
    }

    public String fullyQualifiedLabel() {
        return fullyQualifiedLabel;
    }

    public boolean hasFromAndToLabels() {
        return !fromLabels.isEmpty() && !toLabels.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Label label = (Label) o;
        return fullyQualifiedLabel.equals(label.fullyQualifiedLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullyQualifiedLabel);
    }

    public JsonNode toJson() {

        if (fromLabels.isEmpty() || toLabels.isEmpty()) {
            return JsonNodeFactory.instance.textNode(label);
        }

        ObjectNode labelNode = JsonNodeFactory.instance.objectNode();
        ArrayNode startLabels = JsonNodeFactory.instance.arrayNode();
        ArrayNode endLabels = JsonNodeFactory.instance.arrayNode();

        labelNode.put("name", label);

        for (String fromLabel : fromLabels) {
            startLabels.add(fromLabel);
        }
        labelNode.set("fromLabels", startLabels);

        for (String toLabel : toLabels) {
            endLabels.add(toLabel);
        }
        labelNode.set("toLabels", endLabels);

        return labelNode;
    }
}
