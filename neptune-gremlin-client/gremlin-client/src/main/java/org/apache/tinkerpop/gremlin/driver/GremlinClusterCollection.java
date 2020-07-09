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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GremlinClusterCollection {
    private final Cluster parentCluster;
    private final Map<String, Cluster> clusters = new ConcurrentHashMap<>();
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);

    public GremlinClusterCollection(Cluster parentCluster) {
        this.parentCluster = parentCluster;
    }

    public boolean containsAddress(String address) {
        return clusters.containsKey(address);
    }

    public void add(String address, Cluster cluster) {
        clusters.put(address, cluster);
    }

    public Cluster remove(String address) {
        return clusters.remove(address);
    }

    public Cluster getParentCluster() {
        return parentCluster;
    }

    public CompletableFuture<Void> closeAsync() {

        if (closing.get() != null)
            return closing.get();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Cluster cluster : clusters.values()) {
            futures.add(cluster.closeAsync());
        }
        futures.add(parentCluster.closeAsync());

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    @Override
    public String toString() {
        return clusters.entrySet().stream()
                .map(e -> String.format("  {%s, %s, isClosed: %s}",
                        e.getKey(),
                        e.getValue().allHosts().stream().map(h -> h.getHostUri().toString()).collect(Collectors.joining(",")),
                        e.getValue().isClosed()))
                .collect(Collectors.joining(System.lineSeparator()));
    }
}
