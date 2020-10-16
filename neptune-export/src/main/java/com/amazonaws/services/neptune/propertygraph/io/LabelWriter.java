package com.amazonaws.services.neptune.propertygraph.io;

public interface LabelWriter<T> extends GraphElementHandler<T> {
    String id();
}
