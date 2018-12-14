package com.amazonaws.services.neptune.graph;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Collection;

public class NeptuneClient implements AutoCloseable {

    public static final int DEFAULT_BATCH_SIZE = 64;

    public static NeptuneClient create(Collection<String> endpoints, int port, ConcurrencyConfig concurrencyConfig, boolean useIamAuth) {
        return create(endpoints, port, concurrencyConfig, DEFAULT_BATCH_SIZE, useIamAuth);
    }

    public static NeptuneClient create(Collection<String> endpoints, int port, ConcurrencyConfig concurrencyConfig, int batchSize, boolean useIamAuth) {
        Cluster.Builder builder = Cluster.build()
                .port(port)
                .serializer(Serializers.GRYO_V3D0)
                .maxWaitForConnection(10000)
                .resultIterationBatchSize(batchSize);

        if (useIamAuth){
            builder = builder.channelizer(SigV4WebSocketChannelizer.class);
        }

        for (String endpoint : endpoints) {
            builder = builder.addContactPoint(endpoint);
        }

        return new NeptuneClient(concurrencyConfig.applyTo(builder).create());
    }

    private final Cluster cluster;

    private NeptuneClient(Cluster cluster) {
        this.cluster = cluster;
    }

    public GraphTraversalSource newTraversalSource() {
        return EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    public QueryClient queryClient(){
        return new QueryClient(cluster.connect());
    }

    @Override
    public void close() throws Exception {
        if (cluster != null && !cluster.isClosed() && !cluster.isClosing()) {
            cluster.close();
        }
    }

    public static class QueryClient implements AutoCloseable{

        private final Client client;

        QueryClient(Client client) {
            this.client = client;
        }

        public ResultSet submit(String  gremlin){
            return client.submit(gremlin);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }

}
