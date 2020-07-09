/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package org.apache.tinkerpop.gremlin.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class GremlinCluster implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GremlinCluster.class);

    private final List<String> defaultAddresses;
    private final Function<String, Cluster> clusterBuilder;
    private final Collection<GremlinClusterCollection> clusterCollections = new CopyOnWriteArrayList<>();
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
    private final int refreshOnErrorThreshold;
    private final Supplier<Collection<String>> refreshOnErrorEventHandler;

    public GremlinCluster(List<String> defaultAddresses,
                          Function<String, Cluster> clusterBuilder,
                          int refreshOnErrorThreshold,
                          Supplier<Collection<String>> refreshOnErrorEventHandler) {
        logger.info("Created TopologyAwareCluster, defaultAddresses: {}", defaultAddresses);
        this.defaultAddresses = defaultAddresses;
        this.clusterBuilder = clusterBuilder;
        this.refreshOnErrorThreshold = refreshOnErrorThreshold;
        this.refreshOnErrorEventHandler = refreshOnErrorEventHandler;
    }

    public GremlinClient connect(List<String> addresses, Client.Settings settings) {

        logger.info("Connecting with: {}", addresses);

        if (addresses.isEmpty()){
            throw new IllegalStateException("You must supply at least one address");
        }

        Cluster parentCluster = clusterBuilder.apply(null);

        GremlinClusterCollection clusterCollection = new GremlinClusterCollection(parentCluster);
        List<GremlinClient.ClientHolder> clientHolders = new ArrayList<>();

        for (String address : addresses) {
            Cluster cluster = clusterBuilder.apply(address);
            clientHolders.add(new GremlinClient.ClientHolder(address, cluster.connect()));
            clusterCollection.add(address, cluster);
        }

        clusterCollections.add(clusterCollection);

        return new GremlinClient(
                clusterCollection.getParentCluster(),
                settings,
                clientHolders,
                clusterCollection,
                clusterBuilder,
                refreshOnErrorThreshold,
                refreshOnErrorEventHandler
        );
    }

    public GremlinClient connect(List<String> addresses) {
        return connect(addresses, Client.Settings.build().create());
    }

    public GremlinClient connect() {
        return connect(defaultAddresses, Client.Settings.build().create());
    }

    public GremlinClient connect(Client.Settings settings) {
        return connect(defaultAddresses, settings);
    }

    public CompletableFuture<Void> closeAsync() {

        if (closing.get() != null)
            return closing.get();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (GremlinClusterCollection clusterCollection : clusterCollections) {
            futures.add(clusterCollection.closeAsync());
        }

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    @Override
    public void close() throws Exception {
        closeAsync().join();
    }
}
