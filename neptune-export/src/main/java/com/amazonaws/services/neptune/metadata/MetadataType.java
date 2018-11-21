package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.graph.GraphClient;
import com.amazonaws.services.neptune.io.WriterFactory;
import com.amazonaws.services.neptune.io.Directories;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface MetadataType<T> {
    String name();
    GraphClient<T> graphClient(GraphTraversalSource g);
    WriterFactory<T> writerFactory(Directories directories);
}
