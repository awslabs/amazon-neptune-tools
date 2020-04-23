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

import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.auth.ConnectionConfig;
import com.amazonaws.services.neptune.model.*;
import com.amazonaws.services.neptune.util.Timer;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class CloneCluster implements CloneClusterStrategy {

    @Override
    public Cluster cloneCluster(ConnectionConfig connectionConfig) throws Exception {

        if (!connectionConfig.isDirectConnection()) {
            throw new IllegalStateException("neptune-export does not support cloning a Neptune cluster accessed via a load balancer");
        }

        try (Timer timer = new Timer("cloning cluster")) {

            System.err.println("Cloning cluster...");
            System.err.println();

            AmazonNeptune neptune = AmazonNeptuneClientBuilder.defaultClient();

            NeptuneClusterMetadata sourceClusterMetadata =
                    NeptuneClusterMetadata.createFromEndpoint(connectionConfig.endpoints().iterator().next());

            String sourceClusterId = sourceClusterMetadata.clusterId();
            String targetClusterId = String.format("neptune-export-cluster-%s", UUID.randomUUID().toString().substring(0, 5));
            String instanceType = sourceClusterMetadata.instanceMetadataFor(sourceClusterMetadata.primary()).instanceType();

            System.err.println(String.format("Source clusterId           : %s", sourceClusterId));
            System.err.println(String.format("Target clusterId           : %s", targetClusterId));
            System.err.println(String.format("Instance type              : %s", instanceType));

            DBClusterParameterGroup dbClusterParameterGroup = neptune.createDBClusterParameterGroup(
                    new CreateDBClusterParameterGroupRequest()
                            .withDBClusterParameterGroupName(String.format("%s-db-cluster-params", targetClusterId))
                            .withDescription(String.format("%s DB Cluster Parameter Group", targetClusterId))
                            .withDBParameterGroupFamily("neptune1"));

            neptune.modifyDBClusterParameterGroup(new ModifyDBClusterParameterGroupRequest()
                    .withDBClusterParameterGroupName(dbClusterParameterGroup.getDBClusterParameterGroupName())
                    .withParameters(
                            new Parameter()
                                    .withParameterName("neptune_enforce_ssl")
                                    .withParameterValue("1")
                                    .withApplyMethod(ApplyMethod.PendingReboot),
                            new Parameter()
                                    .withParameterName("neptune_query_timeout")
                                    .withParameterValue("2147483647")
                                    .withApplyMethod(ApplyMethod.PendingReboot)));

            List<Parameter> dbClusterParameters = neptune.describeDBClusterParameters(
                    new DescribeDBClusterParametersRequest()
                            .withDBClusterParameterGroupName(dbClusterParameterGroup.getDBClusterParameterGroupName()))
                    .getParameters();

            while (dbClusterParameters.stream().noneMatch(parameter ->
                    parameter.getParameterName().equals("neptune_query_timeout") &&
                            parameter.getParameterValue().equals("2147483647"))) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dbClusterParameters = neptune.describeDBClusterParameters(
                        new DescribeDBClusterParametersRequest()
                                .withDBClusterParameterGroupName(dbClusterParameterGroup.getDBClusterParameterGroupName()))
                        .getParameters();
            }


            System.err.println(String.format("DB cluster parameter group : %s", dbClusterParameterGroup.getDBClusterParameterGroupName()));

            DBParameterGroup dbParameterGroup = neptune.createDBParameterGroup(
                    new CreateDBParameterGroupRequest()
                            .withDBParameterGroupName(String.format("%s-db-params", targetClusterId))
                            .withDescription(String.format("%s DB Parameter Group", targetClusterId))
                            .withDBParameterGroupFamily("neptune1"));

            neptune.modifyDBParameterGroup(new ModifyDBParameterGroupRequest()
                    .withDBParameterGroupName(dbParameterGroup.getDBParameterGroupName())
                    .withParameters(
                            new Parameter()
                                    .withParameterName("neptune_query_timeout")
                                    .withParameterValue("2147483647")
                                    .withApplyMethod(ApplyMethod.PendingReboot)));

            List<Parameter> dbParameters = neptune.describeDBParameters(
                    new DescribeDBParametersRequest()
                            .withDBParameterGroupName(dbParameterGroup.getDBParameterGroupName()))
                    .getParameters();

            while (dbParameters.stream().noneMatch(parameter ->
                    parameter.getParameterName().equals("neptune_query_timeout") &&
                            parameter.getParameterValue().equals("2147483647"))) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dbParameters = neptune.describeDBClusterParameters(
                        new DescribeDBClusterParametersRequest()
                                .withDBClusterParameterGroupName(dbParameterGroup.getDBParameterGroupName()))
                        .getParameters();
            }

            System.err.println(String.format("DB parameter group         : %s", dbParameterGroup.getDBParameterGroupName()));
            System.err.println();

            System.err.println("Creating target cluster...");

            RestoreDBClusterToPointInTimeRequest cloneClusterRequest = new RestoreDBClusterToPointInTimeRequest()
                    .withSourceDBClusterIdentifier(sourceClusterId)
                    .withDBClusterIdentifier(targetClusterId)
                    .withRestoreType("copy-on-write")
                    .withUseLatestRestorableTime(true)
                    .withPort(sourceClusterMetadata.port())
                    .withDBClusterParameterGroupName(dbClusterParameterGroup.getDBClusterParameterGroupName())
                    .withEnableIAMDatabaseAuthentication(sourceClusterMetadata.isIAMDatabaseAuthenticationEnabled())
                    .withDBSubnetGroupName(sourceClusterMetadata.dbSubnetGroupName())
                    .withVpcSecurityGroupIds(sourceClusterMetadata.vpcSecurityGroupIds())
                    .withTags(
                            new Tag()
                                    .withKey("source")
                                    .withValue(sourceClusterMetadata.clusterId()),
                            new Tag()
                                    .withKey("application")
                                    .withValue("neptune-export"));

            DBCluster targetDbCluster = neptune.restoreDBClusterToPointInTime(cloneClusterRequest);

            String clusterStatus = targetDbCluster.getStatus();

            while (clusterStatus.equals("creating")) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                clusterStatus = neptune.describeDBClusters(
                        new DescribeDBClustersRequest()
                                .withDBClusterIdentifier(targetDbCluster.getDBClusterIdentifier()))
                        .getDBClusters()
                        .get(0)
                        .getStatus();
            }

            System.err.println("Creating target instance...");

            DBInstance targetDbInstance = neptune.createDBInstance(new CreateDBInstanceRequest()
                    .withDBInstanceClass(instanceType)
                    .withDBInstanceIdentifier(String.format("neptune-export-instance-%s", UUID.randomUUID().toString().substring(0, 5)))
                    .withDBClusterIdentifier(targetDbCluster.getDBClusterIdentifier())
                    .withDBParameterGroupName(dbParameterGroup.getDBParameterGroupName())
                    .withEngine("neptune")
                    .withTags(new Tag()
                                    .withKey("source")
                                    .withValue(sourceClusterMetadata.clusterId()),
                            new Tag()
                                    .withKey("application")
                                    .withValue("neptune-export")));

            String instanceStatus = targetDbInstance.getDBInstanceStatus();

            while (instanceStatus.equals("creating")) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instanceStatus = neptune.describeDBInstances(new DescribeDBInstancesRequest()
                        .withDBInstanceIdentifier(targetDbInstance.getDBInstanceIdentifier()))
                        .getDBInstances()
                        .get(0)
                        .getDBInstanceStatus();
            }

            System.err.println();
            System.err.println(String.format("Cluster endpoint: %s", targetDbCluster.getEndpoint()));

            neptune.shutdown();

            return new ClonedCluster(new ConnectionConfig(
                    Collections.singletonList(targetDbCluster.getEndpoint()),
                    connectionConfig.port(),
                    connectionConfig.nlbEndpoint(),
                    connectionConfig.albEndpoint(),
                    connectionConfig.lbPort(),
                    connectionConfig.useIamAuth(),
                    true
            ));
        }
    }

    private static class ClonedCluster implements Cluster {

        private final ConnectionConfig connectionConfig;

        private ClonedCluster(ConnectionConfig connectionConfig) {
            this.connectionConfig = connectionConfig;
        }

        @Override
        public ConnectionConfig connectionConfig() {
            return connectionConfig;
        }

        @Override
        public void close() throws Exception {
            try (Timer timer = new Timer("deleting cloned cluster", false)) {

                System.err.println();
                System.err.println("Deleting cloned cluster...");

                AmazonNeptune neptune = AmazonNeptuneClientBuilder.defaultClient();

                System.err.println("Deleting instance...");

                NeptuneClusterMetadata clusterMetadata =
                        NeptuneClusterMetadata.createFromEndpoint(connectionConfig.endpoints().iterator().next());

                neptune.deleteDBInstance(new DeleteDBInstanceRequest()
                        .withDBInstanceIdentifier(clusterMetadata.primary())
                        .withSkipFinalSnapshot(true));

                try {
                    boolean instanceIsBeingDeleted = neptune.describeDBInstances(
                            new DescribeDBInstancesRequest().withDBInstanceIdentifier(clusterMetadata.primary()))
                            .getDBInstances()
                            .size() > 0;

                    while (instanceIsBeingDeleted) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        instanceIsBeingDeleted = neptune.describeDBInstances(
                                new DescribeDBInstancesRequest().withDBInstanceIdentifier(clusterMetadata.primary()))
                                .getDBInstances()
                                .size() > 0;
                    }
                } catch (DBInstanceNotFoundException e) {
                    // Do nothing
                }

                System.err.println("Deleting cluster...");

                neptune.deleteDBCluster(new DeleteDBClusterRequest()
                        .withDBClusterIdentifier(clusterMetadata.clusterId())
                        .withSkipFinalSnapshot(true));

                try {

                    boolean clusterIsBeingDeleted = neptune.describeDBClusters(
                            new DescribeDBClustersRequest().withDBClusterIdentifier(clusterMetadata.clusterId()))
                            .getDBClusters()
                            .size() > 0;

                    while (clusterIsBeingDeleted) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        clusterIsBeingDeleted = neptune.describeDBClusters(
                                new DescribeDBClustersRequest().withDBClusterIdentifier(clusterMetadata.clusterId()))
                                .getDBClusters()
                                .size() > 0;
                    }
                } catch (DBClusterNotFoundException e){
                    // Do nothing
                }

                System.err.println("Deleting parameter groups...");

                neptune.deleteDBClusterParameterGroup(new DeleteDBClusterParameterGroupRequest()
                        .withDBClusterParameterGroupName(clusterMetadata.dbClusterParameterGroupName()));

                neptune.deleteDBParameterGroup(new DeleteDBParameterGroupRequest()
                        .withDBParameterGroupName(
                                clusterMetadata.instanceMetadataFor(clusterMetadata.primary()).dbParameterGroupName()));

                neptune.shutdown();
            }
        }
    }
}
