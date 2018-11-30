package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;

import java.io.IOException;
import java.util.Map;

public interface WriterFactory<T> {

    Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata, Format format) throws IOException;

    GraphElementHandler<T> createLabelWriter(Printer printer);

}
