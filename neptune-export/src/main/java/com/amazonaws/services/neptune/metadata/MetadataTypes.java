package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.graph.EdgesClient;
import com.amazonaws.services.neptune.graph.GraphClient;
import com.amazonaws.services.neptune.graph.NodesClient;
import com.amazonaws.services.neptune.io.EdgesWriterFactory;
import com.amazonaws.services.neptune.io.NodesWriterFactory;
import com.amazonaws.services.neptune.io.WriterFactory;
import com.amazonaws.services.neptune.io.Directories;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class MetadataTypes {
    public static final MetadataType<Map<?, Object>> Nodes = new MetadataType<Map<?, Object>>() {
        @Override
        public String name() {
            return "nodes";
        }

        @Override
        public GraphClient<Map<?, Object>> graphClient(GraphTraversalSource g) {
            return new NodesClient(g);
        }

        @Override
        public WriterFactory<Map<?, Object>> writerFactory(Directories directories) {
            return new NodesWriterFactory(directories);
        }
    };

    public static final MetadataType<Path> Edges = new MetadataType<Path>() {
        @Override
        public String name() {
            return "edges";
        }

        @Override
        public GraphClient<Path> graphClient(GraphTraversalSource g) {
            return new EdgesClient(g);
        }

        @Override
        public WriterFactory<Path> writerFactory(Directories directories) {
            return new EdgesWriterFactory(directories);
        }
    };

    public static Collection<MetadataType<?>> values() {
        return Arrays.asList(Nodes, Edges);
    }
}
