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
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AddCloneTask {

    private final String sourceClusterId;
    private final String targetClusterId;
    private final String cloneClusterInstanceType;
    private final int replicaCount;
    private final String engineVersion;

    public AddCloneTask(String sourceClusterId,
                        String targetClusterId,
                        String cloneClusterInstanceType,
                        int replicaCount,
                        String engineVersion) {
        this.sourceClusterId = sourceClusterId;
        this.targetClusterId = targetClusterId;
        this.cloneClusterInstanceType = cloneClusterInstanceType;
        this.replicaCount = replicaCount;
        this.engineVersion = engineVersion;
    }

    public NeptuneClusterMetadata execute() {

        try (Timer timer = new Timer("cloning cluster")) {

            System.err.println("Cloning cluster " + sourceClusterId + "...");
            System.err.println();

            NeptuneClusterMetadata sourceClusterMetadata = NeptuneClusterMetadata.createFromClusterId(sourceClusterId);

            InstanceType instanceType = StringUtils.isEmpty(cloneClusterInstanceType) ?
                    InstanceType.parse(sourceClusterMetadata.instanceMetadataFor(sourceClusterMetadata.primary()).instanceType()) :
                    InstanceType.parse(cloneClusterInstanceType);

            System.err.println(String.format("Source clusterId           : %s", sourceClusterId));
            System.err.println(String.format("Target clusterId           : %s", targetClusterId));
            System.err.println(String.format("Target instance type       : %s", instanceType));

            AmazonNeptune neptune = AmazonNeptuneClientBuilder.defaultClient();

            DBClusterParameterGroup dbClusterParameterGroup;
            DBParameterGroup dbParameterGroup;

            try (Timer paramGroupsTime = new Timer("creating parameter groups")) {

                dbClusterParameterGroup = neptune.createDBClusterParameterGroup(
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

                dbParameterGroup = neptune.createDBParameterGroup(
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
            }

            DBCluster targetDbCluster;

            try (Timer clusterTimer = new Timer("creating target cluster")) {

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
                                        .withValue(NeptuneClusterMetadata.NEPTUNE_EXPORT_APPLICATION_TAG));

                targetDbCluster = neptune.restoreDBClusterToPointInTime(cloneClusterRequest);

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
            }

            try (Timer primaryTimer = new Timer("creating primary")) {

                createInstance("primary",
                        neptune,
                        sourceClusterMetadata,
                        instanceType,
                        dbParameterGroup,
                        targetDbCluster);
            }

            if (replicaCount > 0) {

                try (Timer primaryTimer = new Timer("creating replicas")) {

                    ExecutorService taskExecutor = Executors.newFixedThreadPool(replicaCount);

                    for (int i = 0; i < replicaCount; i++) {

                        taskExecutor.execute(() -> createInstance("replica",
                                neptune,
                                sourceClusterMetadata,
                                instanceType,
                                dbParameterGroup,
                                targetDbCluster));
                    }

                    taskExecutor.shutdown();

                    try {
                        taskExecutor.awaitTermination(30, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            }

            neptune.shutdown();

            return NeptuneClusterMetadata.createFromClusterId(targetClusterId);
        }
    }

    private void createInstance(String name,
                                AmazonNeptune neptune,
                                NeptuneClusterMetadata sourceClusterMetadata,
                                InstanceType instanceType,
                                DBParameterGroup dbParameterGroup,
                                DBCluster targetDbCluster) {

        System.err.println("Creating target " + name + " instance...");

        CreateDBInstanceRequest request = new CreateDBInstanceRequest()
                .withDBInstanceClass(instanceType.value())
                .withDBInstanceIdentifier(String.format("neptune-export-%s-%s", name, UUID.randomUUID().toString().substring(0, 5)))
                .withDBClusterIdentifier(targetDbCluster.getDBClusterIdentifier())
                .withDBParameterGroupName(dbParameterGroup.getDBParameterGroupName())
                .withEngine("neptune")
                .withTags(new Tag()
                                .withKey("source")
                                .withValue(sourceClusterMetadata.clusterId()),
                        new Tag()
                                .withKey("application")
                                .withValue(NeptuneClusterMetadata.NEPTUNE_EXPORT_APPLICATION_TAG));

        if (StringUtils.isNotEmpty(engineVersion)){
            request = request.withEngineVersion(engineVersion);
        }

        DBInstance targetDbInstance = neptune.createDBInstance(request);

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
    }

}
