package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.io.GraphElementHandler;

import java.util.Collection;
import java.util.Map;

public interface GraphClient<T> {
    String description();

    void queryForMetadata(GraphElementHandler<Map<?, Object>> handler, Range range, LabelsFilter labelsFilter);

    void queryForValues(GraphElementHandler<T> handler, Range range, LabelsFilter labelsFilter);

    long count(LabelsFilter labelsFilter);

    Collection<String> labels();

    String getLabelFrom(T input);
}
