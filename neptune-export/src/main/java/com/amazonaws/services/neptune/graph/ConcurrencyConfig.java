package com.amazonaws.services.neptune.graph;

import org.apache.tinkerpop.gremlin.driver.Cluster;

import static java.lang.Math.max;

public class ConcurrencyConfig {
    private final int concurrency;
    private final long range;

    public ConcurrencyConfig(int concurrency, long range) {

        if (concurrency < 1){
            throw new IllegalArgumentException("Concurrency must be >= 1");
        }

        this.concurrency = concurrency;
        this.range = range;
    }

    public int concurrency() {
        return concurrency;
    }

    long range() {
        return range;
    }

    boolean isUnboundedParallelExecution(){
        return concurrency > 1 && range == -1;
    }

    Cluster.Builder applyTo(Cluster.Builder clusterBuilder){
        if (concurrency == 1){
            return clusterBuilder;
        }

        int minPoolSize = max(concurrency/2, 2);
        int maxPoolSize =  max(concurrency/2, 8);
        int minSimultaneousUsage = max(concurrency/2, 8);
        int maxSimultaneousUsage = max(concurrency, 16);
        int minInProcess = max(concurrency/8, 1);
        int maxInProcess = max(concurrency/4, 4);

        return clusterBuilder.
                minConnectionPoolSize(minPoolSize).
                maxConnectionPoolSize(maxPoolSize).
                minSimultaneousUsagePerConnection(minSimultaneousUsage).
                maxSimultaneousUsagePerConnection(maxSimultaneousUsage).
                minInProcessPerConnection(minInProcess).
                maxInProcessPerConnection(maxInProcess);
    }
}
