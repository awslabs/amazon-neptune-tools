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

import org.apache.tinkerpop.gremlin.driver.Cluster;

import static java.lang.Math.max;

public class ConcurrencyConfig {
    private final int concurrency;
    private final long rangeSize;
    private final long skip;
    private final long limit;

    public ConcurrencyConfig(int concurrency) {

        this(concurrency, -1, 0, Long.MAX_VALUE);
    }

    public ConcurrencyConfig(int concurrency, long rangeSize, long skip, long limit) {

        if (concurrency < 1){
            throw new IllegalArgumentException("Concurrency must be >= 1");
        }

        this.concurrency = concurrency;
        this.rangeSize = rangeSize;
        this.skip = skip;
        this.limit = limit;
    }

    public int concurrency() {
        return concurrency;
    }

    long rangeSize() {
        return rangeSize;
    }

    public long skip() {
        return skip;
    }

    public long limit() {
        return limit;
    }

    boolean isUnboundedParallelExecution(){
        return concurrency > 1 && rangeSize == -1;
    }

    Cluster.Builder applyTo(Cluster.Builder clusterBuilder){
        if (concurrency == 1){
            return clusterBuilder;
        }

        int minPoolSize = max(concurrency, 2);
        int maxPoolSize =  max(concurrency, 8);
        int minSimultaneousUsage = 1;
        int maxSimultaneousUsage = 1;
        int minInProcess = 1;
        int maxInProcess = 1;

        return clusterBuilder.
                minConnectionPoolSize(minPoolSize).
                maxConnectionPoolSize(maxPoolSize).
                minSimultaneousUsagePerConnection(minSimultaneousUsage).
                maxSimultaneousUsagePerConnection(maxSimultaneousUsage).
                minInProcessPerConnection(minInProcess).
                maxInProcessPerConnection(maxInProcess);
    }
}
