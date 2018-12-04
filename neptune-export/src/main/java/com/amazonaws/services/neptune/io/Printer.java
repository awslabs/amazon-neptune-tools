package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface Printer extends AutoCloseable {
    void printHeaderMandatoryColumns(String... columns);
    void printHeaderRemainingColumns(Collection<PropertyTypeInfo> remainingColumns);
    void printProperties(Map<?, ?> properties) throws IOException;
    void printEdge(String id, String label, String from, String to) throws IOException;
    void printNode(String id, String label) throws IOException;
    void printStartRow() throws IOException;
    void printEndRow() throws IOException;
}
