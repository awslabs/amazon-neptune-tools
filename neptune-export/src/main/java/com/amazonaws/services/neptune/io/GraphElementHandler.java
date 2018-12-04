package com.amazonaws.services.neptune.io;

import java.io.IOException;

public interface GraphElementHandler<T> extends AutoCloseable {
    void handle(T element, boolean allowStructuralElements) throws IOException;
}
