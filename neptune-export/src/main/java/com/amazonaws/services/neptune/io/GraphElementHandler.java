package com.amazonaws.services.neptune.io;

public interface GraphElementHandler<T> extends AutoCloseable {
    void handle(T element, boolean allowStructuralElements);
}
