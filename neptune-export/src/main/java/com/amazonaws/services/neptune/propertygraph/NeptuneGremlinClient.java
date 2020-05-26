/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph;

import com.amazonaws.services.neptune.cluster.ClusterStrategy;
import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.cluster.ConnectionConfig;
import com.amazonaws.services.neptune.propertygraph.io.SerializationConfig;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

public class NeptuneGremlinClient implements AutoCloseable {

    public static final int DEFAULT_BATCH_SIZE = 64;

    public static NeptuneGremlinClient create(ClusterStrategy clusterStrategy, SerializationConfig serializationConfig) {
        ConnectionConfig connectionConfig = clusterStrategy.connectionConfig();
        ConcurrencyConfig concurrencyConfig = clusterStrategy.concurrencyConfig();

        Cluster.Builder builder = Cluster.build()
                .port(connectionConfig.port())
                .enableSsl(connectionConfig.useSsl())
                .serializer(serializationConfig.serializer())
                .maxWaitForConnection(10000)
                .resultIterationBatchSize(serializationConfig.batchSize())
                .maxContentLength(serializationConfig.maxContentLength());

        if (connectionConfig.useIamAuth()) {
            if (connectionConfig.isDirectConnection()) {
                builder = builder.channelizer(SigV4WebSocketChannelizer.class);
            } else {
                builder = builder
                        // use the JAAS_ENTRY auth property to pass Host header info to the channelizer
                        .authProperties(new AuthProperties().with(AuthProperties.Property.JAAS_ENTRY, connectionConfig.handshakeRequestConfig().value()))
                        .channelizer(LBAwareSigV4WebSocketChannelizer.class);
            }
        }

        for (String endpoint : connectionConfig.endpoints()) {
            builder = builder.addContactPoint(endpoint);
        }

        int numberOfEndpoints = connectionConfig.endpoints().size();

        return new NeptuneGremlinClient(concurrencyConfig.applyTo(builder, numberOfEndpoints).create());
    }

    private final Cluster cluster;

    private NeptuneGremlinClient(Cluster cluster) {
        this.cluster = cluster;
    }

    public GraphTraversalSource newTraversalSource() {
        return EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    public QueryClient queryClient() {
        return new QueryClient(cluster.connect());
    }

    @Override
    public void close() throws Exception {
        if (cluster != null && !cluster.isClosed() && !cluster.isClosing()) {
            cluster.close();
        }
    }

    public static class QueryClient implements AutoCloseable {

        private final Client client;

        QueryClient(Client client) {
            this.client = client;
        }

        public ResultSet submit(String gremlin) {
            return client.submit(gremlin);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }

}
