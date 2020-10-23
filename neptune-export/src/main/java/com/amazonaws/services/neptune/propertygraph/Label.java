package com.amazonaws.services.neptune.propertygraph;

import com.amazonaws.services.neptune.propertygraph.schema.GraphElementSchemas;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class Label {

    public static Label fromJson(JsonNode jsonNode) {
        if (jsonNode.isContainerNode()) {
            ArrayNode startLabels = (ArrayNode) jsonNode.path("startLabels");
            String label = jsonNode.path("name").textValue();
            ArrayNode endLabels = (ArrayNode) jsonNode.path("endLabels");
            Collection<String> preLabels = new ArrayList<>();
            startLabels.forEach(l -> preLabels.add(l.textValue()));
            Collection<String> postLabels = new ArrayList<>();
            endLabels.forEach(l -> postLabels.add(l.textValue()));
            return new Label(label, preLabels, postLabels);
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
    private final Collection<String> preLabels;
    private final Collection<String> postLabels;

    public Label(String label) {
        this(label, Collections.emptyList(), Collections.emptyList());
    }

    public Label(String label, Collection<String> preLabels, Collection<String> postLabels) {
        this.label = label;
        this.preLabels = preLabels;
        this.postLabels = postLabels;
        this.fullyQualifiedLabel = preLabels.isEmpty() || postLabels.isEmpty() ?
                label :
                String.format("(%s)-[%s]->(%s)", String.join("|", preLabels), label, String.join("|", postLabels));
    }

    public String label() {
        return label;
    }

    public String fullyQualifiedLabel() {
        return fullyQualifiedLabel;
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

        if (preLabels.isEmpty() || postLabels.isEmpty()) {
            return JsonNodeFactory.instance.textNode(label);
        }

        ObjectNode labelNode = JsonNodeFactory.instance.objectNode();
        ArrayNode startLabels = JsonNodeFactory.instance.arrayNode();
        ArrayNode endLabels = JsonNodeFactory.instance.arrayNode();

        labelNode.put("name", label);

        for (String preLabel : preLabels) {
            startLabels.add(preLabel);
        }
        labelNode.set("startLabels", startLabels);

        for (String postLabel : postLabels) {
            endLabels.add(postLabel);
        }
        labelNode.set("endLabels", endLabels);

        return labelNode;
    }
}
