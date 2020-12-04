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

import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.stream.Collectors;

public class Label {

    private static final String SEMICOLON_SEPARATOR = "(?<!\\\\);";

    public static Collection<String> split(String s){
        return Arrays.asList(s.split(SEMICOLON_SEPARATOR));
    }

    public static List<String> fixLabelsIssue(List<String> list) {
        if (list.size() == 1 && list.get(0).contains("::")){
            List<String> newResults = Arrays.asList(list.get(0).split("::"));
            newResults.sort(String::compareTo);
            return newResults;
        }
        return list;
    }

    public static Label fromJson(JsonNode jsonNode) {
        if (jsonNode.isObject()) {

            ArrayNode fromLabelsArrays = (ArrayNode) jsonNode.path("~fromLabels");
            String label =  jsonNode.path("~label").textValue();
            ArrayNode toLabelsArray = (ArrayNode) jsonNode.path("~toLabels");

            Collection<String> fromLabels = new ArrayList<>();
            fromLabelsArrays.forEach(l -> fromLabels.add(l.textValue()));

            Collection<String> toLabels = new ArrayList<>();
            toLabelsArray.forEach(l -> toLabels.add(l.textValue()));

            return new Label(Collections.singletonList(label), fromLabels, toLabels);
        } else {
            if (jsonNode.isArray()){
                ArrayNode labelsNode = (ArrayNode) jsonNode;
                Collection<String> labels = new ArrayList<>();
                labelsNode.forEach(l -> labels.add(l.textValue()));
                return new Label(labels);
            } else {
                return new Label(Collections.singletonList(jsonNode.textValue()));
            }

        }
    }

    public static Collection<Label> forLabels(Collection<String> labels) {
        Set<Label> results = new HashSet<>();
        for (String label : labels) {
            results.add(new Label(Collections.singletonList(label)));
        }
        return results;
    }

    private final List<String> labels;
    private final List<String> fromLabels;
    private final List<String> toLabels;
    private final String fullyQualifiedLabel;

    public Label(String label) {
        this(split(label));
    }

    public Label(Collection<String> labels) {
        this(labels, Collections.emptyList(), Collections.emptyList());
    }

    public Label(String label, String fromLabels, String toLabels) {
        this(label, split(fromLabels), split(toLabels));
    }

    public Label(String label, Collection<String> fromLabels, Collection<String> toLabels) {
        this(Collections.singletonList(label), fromLabels, toLabels);
    }

    private Label(Collection<String> labels, Collection<String> fromLabels, Collection<String> toLabels) {
        this.labels = labelList(labels);
        this.fromLabels = labelList(fromLabels);
        this.toLabels = labelList(toLabels);

        this.fullyQualifiedLabel = hasFromAndToLabels() ?
                format(fromLabelsAsString(), labelsAsString(), toLabelsAsString()):
                labelsAsString() ;
    }

    private String format(String fromLabels, String label, String toLabels){
        return String.format("(%s)-%s-(%s)", fromLabels, label, toLabels);
    }

    private List<String> escapeSemicolons(List<String> list){
        return list.stream().map(DataType::escapeSemicolons).collect(Collectors.toList());
    }

    private List<String> labelList(Collection<String> col){
        List<String> results = new ArrayList<>(col);
        results = fixLabelsIssue(results);
        results.sort(String::compareTo);
        return results;
    }

    public List<String> label() {
        return labels;
    }

    public String fromLabelsAsString(){
        return String.join(";", escapeSemicolons(fromLabels));
    }

    public String toLabelsAsString(){
        return String.join(";", escapeSemicolons(toLabels));
    }

    public String labelsAsString(){
        return String.join(";", escapeSemicolons(labels));
    }

    public String fullyQualifiedLabel() {
        return fullyQualifiedLabel;
    }

    public boolean hasFromAndToLabels() {
        return !fromLabels.isEmpty() && !toLabels.isEmpty();
    }

    public Label createCopy(){
        return Label.fromJson(toJson());
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

        if (!hasFromAndToLabels()) {
            if (labels.size() > 1){
                ArrayNode labelsArray = JsonNodeFactory.instance.arrayNode();
                for (String label : labels) {
                    labelsArray.add(label);
                }
                return labelsArray;
            } else {
                return JsonNodeFactory.instance.textNode(labels.get(0));
            }
        }


        ObjectNode labelNode = JsonNodeFactory.instance.objectNode();

        ArrayNode fromLabelsArray = JsonNodeFactory.instance.arrayNode();
        ArrayNode toLabelsArray = JsonNodeFactory.instance.arrayNode();

        labelNode.put("~label", labels.get(0));

        for (String fromLabel : fromLabels) {
            fromLabelsArray.add(fromLabel);
        }
        labelNode.set("~fromLabels", fromLabelsArray);

        for (String toLabel : toLabels) {
            toLabelsArray.add(toLabel);
        }
        labelNode.set("~toLabels", toLabelsArray);

        return labelNode;
    }
}
