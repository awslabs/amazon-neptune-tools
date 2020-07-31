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

package com.amazonaws.services.neptune.cluster;

import java.util.UUID;

public class CloneCluster implements CloneClusterStrategy {

    private final String cloneClusterInstanceType;
    private final int replicaCount;
    private final int maxConcurrency;
    private final String engineVersion;

    public CloneCluster(String cloneClusterInstanceType, int replicaCount, int maxConcurrency, String engineVersion) {
        this.cloneClusterInstanceType = cloneClusterInstanceType;
        this.replicaCount = replicaCount;
        this.maxConcurrency = maxConcurrency;
        this.engineVersion = engineVersion;
    }

    @Override
    public ClusterStrategy cloneCluster(ConnectionConfig connectionConfig, ConcurrencyConfig concurrencyConfig) throws Exception {

        if (!connectionConfig.isDirectConnection()) {
            throw new IllegalStateException("neptune-export does not support cloning a Neptune cluster accessed via a load balancer");
        }

        String clusterId = NeptuneClusterMetadata.clusterIdFromEndpoint(connectionConfig.endpoints().iterator().next());
        String targetClusterId = String.format("neptune-export-cluster-%s", UUID.randomUUID().toString().substring(0, 5));

        AddCloneTask addCloneTask = new AddCloneTask(clusterId, targetClusterId, cloneClusterInstanceType, replicaCount, engineVersion);
        NeptuneClusterMetadata targetClusterMetadata = addCloneTask.execute();

        InstanceType instanceType =  InstanceType.parse(
                targetClusterMetadata.instanceMetadataFor(targetClusterMetadata.primary()).instanceType());

        int targetConcurrency = instanceType.concurrency() * (1 + replicaCount);
        int newConcurrency = maxConcurrency > 0 ?
                Math.min(maxConcurrency, targetConcurrency) :
                targetConcurrency;

        System.err.println();

        System.err.println(String.format("Endpoints       : %s", String.join(", ", targetClusterMetadata.endpoints())));
        System.err.println(String.format("Max concurrency : %s", maxConcurrency));
        System.err.println(String.format("Concurrency     : %s", newConcurrency));

        return new ClonedClusterStrategy(
                targetClusterId,
                new ConnectionConfig(
                        targetClusterMetadata.endpoints(),
                        connectionConfig.port(),
                        connectionConfig.nlbEndpoint(),
                        connectionConfig.albEndpoint(),
                        connectionConfig.lbPort(),
                        connectionConfig.useIamAuth(),
                        true
                ),
                new ConcurrencyConfig(newConcurrency));
    }

    private static class ClonedClusterStrategy implements ClusterStrategy {

        private final String clusterId;
        private final ConnectionConfig connectionConfig;
        private final ConcurrencyConfig concurrencyConfig;

        private ClonedClusterStrategy(String clusterId,
                                      ConnectionConfig connectionConfig,
                                      ConcurrencyConfig concurrencyConfig) {
            this.clusterId = clusterId;
            this.connectionConfig = connectionConfig;
            this.concurrencyConfig = concurrencyConfig;
        }

        @Override
        public ConnectionConfig connectionConfig() {
            return connectionConfig;
        }

        @Override
        public ConcurrencyConfig concurrencyConfig() {
            return concurrencyConfig;
        }

        @Override
        public void close() throws Exception {

            RemoveCloneTask removeCloneTask = new RemoveCloneTask(clusterId);
            removeCloneTask.execute();
        }
    }
}
