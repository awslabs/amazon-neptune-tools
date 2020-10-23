package com.amazonaws.services.neptune.propertygraph;

import java.util.*;

public class Label {

    public static Collection<Label> forLabels(String... labels){
        return forLabels(Arrays.asList(labels));
    }

    public static Collection<Label> forLabels(Collection<String> labels){
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
        List<String> labels = new ArrayList<>(preLabels);
        labels.add(label);
        labels.addAll(postLabels);
        this.fullyQualifiedLabel = String.join("::", labels);
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
}
