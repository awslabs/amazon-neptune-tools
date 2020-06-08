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

import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.*;
import com.amazonaws.services.neptune.util.Timer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RemoveCloneTask {

    private final String clusterId;

    public RemoveCloneTask(String clusterId) {
        this.clusterId = clusterId;
    }

    public void execute() {

        AmazonNeptune neptuneClient = AmazonNeptuneClientBuilder.defaultClient();

        try (Timer timer = new Timer("deleting cloned cluster", false)) {

            System.err.println();
            System.err.println("Deleting cloned cluster " + clusterId + "...");


            NeptuneClusterMetadata metadata = NeptuneClusterMetadata.createFromClusterId(clusterId);

            if (!metadata.isTaggedWithNeptuneExport()) {
                throw new IllegalStateException("Cluster must have an 'application' tag with the value '" +
                        NeptuneClusterMetadata.NEPTUNE_EXPORT_APPLICATION_TAG + "' before it can be deleted");
            }

            ExecutorService taskExecutor = Executors.newFixedThreadPool(1 + metadata.replicas().size());

            taskExecutor.execute(() -> deleteInstance(neptuneClient, metadata.primary()));

            for (String replicaId : metadata.replicas()) {
                taskExecutor.execute(() -> deleteInstance(neptuneClient, replicaId));
            }

            taskExecutor.shutdown();

            try {
                taskExecutor.awaitTermination(30, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            System.err.println("Deleting cluster...");

            neptuneClient.deleteDBCluster(new DeleteDBClusterRequest()
                    .withDBClusterIdentifier(metadata.clusterId())
                    .withSkipFinalSnapshot(true));

            try {

                boolean clusterIsBeingDeleted = neptuneClient.describeDBClusters(
                        new DescribeDBClustersRequest().withDBClusterIdentifier(metadata.clusterId()))
                        .getDBClusters()
                        .size() > 0;

                while (clusterIsBeingDeleted) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    clusterIsBeingDeleted = neptuneClient.describeDBClusters(
                            new DescribeDBClustersRequest().withDBClusterIdentifier(metadata.clusterId()))
                            .getDBClusters()
                            .size() > 0;
                }
            } catch (DBClusterNotFoundException e) {
                // Do nothing
            }

            System.err.println("Deleting parameter groups...");

            neptuneClient.deleteDBClusterParameterGroup(new DeleteDBClusterParameterGroupRequest()
                    .withDBClusterParameterGroupName(metadata.dbClusterParameterGroupName()));

            neptuneClient.deleteDBParameterGroup(new DeleteDBParameterGroupRequest()
                    .withDBParameterGroupName(
                            metadata.instanceMetadataFor(metadata.primary()).dbParameterGroupName()));

        } finally {
            if (neptuneClient != null) {
                neptuneClient.shutdown();
            }
        }
    }

    private void deleteInstance(AmazonNeptune neptune, String instanceId) {
        System.err.println("Deleting instance " + instanceId + "...");

        neptune.deleteDBInstance(new DeleteDBInstanceRequest()
                .withDBInstanceIdentifier(instanceId)
                .withSkipFinalSnapshot(true));

        try {
            boolean instanceIsBeingDeleted = neptune.describeDBInstances(
                    new DescribeDBInstancesRequest().withDBInstanceIdentifier(instanceId))
                    .getDBInstances()
                    .size() > 0;

            while (instanceIsBeingDeleted) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instanceIsBeingDeleted = neptune.describeDBInstances(
                        new DescribeDBInstancesRequest().withDBInstanceIdentifier(instanceId))
                        .getDBInstances()
                        .size() > 0;
            }
        } catch (DBInstanceNotFoundException e) {
            // Do nothing
        }
    }
}
