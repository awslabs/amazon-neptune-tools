package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.util.Collection;
import java.util.Map;

public interface Printer extends AutoCloseable {
    void printHeaderMandatoryColumns(String... columns);
    void printHeaderRemainingColumns(Collection<PropertyTypeInfo> remainingColumns);
    void printProperties(Map<?, ?> properties);
    void printEdge(String id, String label, String from, String to);
    void printNode(String id, String label);
    void printStartRow();
    void printEndRow();
}
