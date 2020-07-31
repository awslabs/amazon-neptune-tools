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

package com.amazonaws.services.neptune.cluster;

import com.amazonaws.services.neptune.propertygraph.RangeConfig;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import static java.lang.Math.max;

public class ConcurrencyConfig {
    private final int concurrency;

    public ConcurrencyConfig(int concurrency) {

        if (concurrency < 1){
            throw new IllegalArgumentException("Concurrency must be >= 1");
        }

        this.concurrency = concurrency;

    }

    public int concurrency() {
        return concurrency;
    }

    public boolean isUnboundedParallelExecution(RangeConfig rangeConfig){
        return concurrency > 1 && rangeConfig.rangeSize() == -1;
    }

    public Cluster.Builder applyTo(Cluster.Builder clusterBuilder, int numberOfEndpoints){
        if (concurrency == 1){
            return clusterBuilder;
        }

        int calculatedPoolSize = (concurrency/numberOfEndpoints) + 1;

        int minPoolSize = max(calculatedPoolSize, 2);
        int maxPoolSize =  max(calculatedPoolSize, 8);

        return clusterBuilder.
                minConnectionPoolSize(minPoolSize).
                maxConnectionPoolSize(maxPoolSize);
    }
}
