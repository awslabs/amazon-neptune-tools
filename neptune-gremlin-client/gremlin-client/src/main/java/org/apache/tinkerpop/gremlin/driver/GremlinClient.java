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

import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GremlinClient extends Client implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GremlinClient.class);

    private final AtomicReference<List<ClientHolder>> clientHolders = new AtomicReference<>(new ArrayList<>());
    private final AtomicLong index = new AtomicLong(0);
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
    private final AtomicBoolean refreshing = new AtomicBoolean(false);
    private final AtomicInteger consecutiveErrorCount = new AtomicInteger(0);

    private final GremlinClusterCollection clusterCollection;
    private final Function<String, Cluster> clusterBuilder;
    private final int refreshOnErrorThreshold;
    private final Supplier<Collection<String>> refreshOnErrorEventHandler;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    GremlinClient(Cluster cluster,
                  Settings settings,
                  List<ClientHolder> clientHolders,
                  GremlinClusterCollection clusterCollection,
                  Function<String, Cluster> clusterBuilder,
                  int refreshOnErrorThreshold,
                  Supplier<Collection<String>> refreshOnErrorEventHandler) {
        super(cluster, settings);

        this.refreshOnErrorThreshold = refreshOnErrorThreshold;
        this.refreshOnErrorEventHandler = refreshOnErrorEventHandler;
        this.clientHolders.set(clientHolders);
        this.clusterCollection = clusterCollection;
        this.clusterBuilder = clusterBuilder;
    }

    public void refreshEndpoints(String... addresses) {
        refreshEndpoints(Arrays.asList(addresses));
    }

    public synchronized void refreshEndpoints(Collection<String> addresses) {

        if (closing.get() != null) {
            return;
        }

        List<ClientHolder> oldClientHolders = clientHolders.get();
        List<ClientHolder> newClientHolders = new ArrayList<>();
        List<String> addressesToRemove = new ArrayList<>();

        for (ClientHolder clientHolder : oldClientHolders) {
            String address = clientHolder.getAddress();
            if (addresses.contains(address)) {
                newClientHolders.add(clientHolder);
            } else {
                addressesToRemove.add(address);
            }
        }

        for (String address : addresses) {
            if (!clusterCollection.containsAddress(address)) {
                logger.info("Adding client for {}", address);
                Cluster cluster = clusterBuilder.apply(address);
                ClientHolder clientHolder = new ClientHolder(address, cluster.connect());
                clientHolder.init();
                newClientHolders.add(clientHolder);
                clusterCollection.add(address, cluster);
            }
        }

        clientHolders.set(newClientHolders);

        for (String address : addressesToRemove) {
            logger.info("Removing client for {}", address);
            Cluster cluster = clusterCollection.remove(address);
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    @Override
    protected void initializeImplementation() {
        // Do nothing
    }

    @Override
    protected Connection chooseConnection(RequestMessage msg) throws TimeoutException, ConnectionException {

        long start = System.currentTimeMillis();

        logger.debug("Choosing connection");

        Connection connection = null;

        while (connection == null) {

            List<ClientHolder> currentClientHolders = clientHolders.get();

            while (currentClientHolders.isEmpty()) {
                try {
                    Thread.sleep(500);
                    currentClientHolders = clientHolders.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            ClientHolder clientHolder = currentClientHolders.get((int) (index.getAndIncrement() % currentClientHolders.size()));

            if (clientHolder.isAvailable()) {
                connection = clientHolder.chooseConnection(msg);
                resetErrorCount();
            } else {
                logger.warn("Client for {} not available", clientHolder.getAddress());
                handleError();
            }
        }

        logger.debug("Connection: {} [{} ms]", connection.getConnectionInfo(), System.currentTimeMillis() - start);

        return connection;
    }

    private void handleError() {
        if (refreshOnErrorThreshold > 0
                && consecutiveErrorCount.incrementAndGet() > refreshOnErrorThreshold
                && !refreshing.get()) {
            consecutiveErrorCount.set(0);
            if (refreshOnErrorEventHandler != null) {
                executorService.submit(
                        new RefreshOnErrorEventHandler(this, refreshing, refreshOnErrorEventHandler));
            }
        }
    }

    private void resetErrorCount() {
        if (refreshOnErrorThreshold > 0){
            consecutiveErrorCount.set(0);
        }
    }

    @Override
    public boolean isClosing() {
        return closing.get() != null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {

        if (closing.get() != null)
            return closing.get();

        executorService.shutdownNow();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ClientHolder clientHolder : clientHolders.get()) {
            futures.add(clientHolder.closeAsync());
        }

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    @Override
    public synchronized Client init() {
        if (initialized)
            return this;

        logger.debug("Initializing internal clients");

        for (ClientHolder clientHolder : clientHolders.get()) {
            clientHolder.init();
        }
        initializeImplementation();

        initialized = true;
        return this;
    }

    @Override
    public String toString() {

        return "Client holder queue: " + System.lineSeparator() +
                clientHolders.get().stream()
                        .map(c -> String.format("  {address: %s, isAvailable: %s}",
                                c.getAddress(),
                                c.isAvailable()))
                        .collect(Collectors.joining(System.lineSeparator())) +
                System.lineSeparator() +
                "Cluster collection: " + System.lineSeparator() +
                clusterCollection.toString();
    }

    static class ClientHolder {
        private final String host;
        private final Client client;

        public ClientHolder(String host, Client client) {
            this.host = host;
            this.client = client;
        }

        public String getAddress() {
            return host;
        }

        public boolean isAvailable() {
            return !client.getCluster().availableHosts().isEmpty();
        }

        public Connection chooseConnection(RequestMessage msg) throws TimeoutException, ConnectionException {
            try {
                Connection connection = client.chooseConnection(msg);
                if (connection.isClosing()) {
                    logger.warn("Connection is closing: {}", host);
                    return null;
                }
                if (connection.isDead()) {
                    logger.warn("Connection is dead: {}", host);
                    return null;
                }
                return connection;
            } catch (NullPointerException e) {
                logger.warn("NullPointerException: {}", host, e);
                return null;
            }
        }

        public CompletableFuture<Void> closeAsync() {
            return client.closeAsync();
        }

        public void init() {
            client.init();
        }
    }

    private static class RefreshOnErrorEventHandler implements Runnable {

        private final GremlinClient client;
        private final AtomicBoolean refreshing;
        private final Supplier<Collection<String>> refreshOnErrorEventHandler;

        private RefreshOnErrorEventHandler(GremlinClient client,
                                           AtomicBoolean refreshing,
                                           Supplier<Collection<String>> refreshOnErrorEventHandler) {
            this.client = client;
            this.refreshing = refreshing;
            this.refreshOnErrorEventHandler = refreshOnErrorEventHandler;
        }

        @Override
        public void run() {
            boolean isAlreadyRefreshing = refreshing.getAndSet(true);

            if (isAlreadyRefreshing) {
                return;
            }
            Collection<String> endpoints = refreshOnErrorEventHandler.get();
            client.refreshEndpoints(endpoints);

            refreshing.set(false);
        }
    }
}
