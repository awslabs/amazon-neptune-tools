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
import com.amazonaws.services.neptune.model.*;
import com.amazonaws.services.neptune.util.Activity;
import com.amazonaws.services.neptune.util.EnvironmentVariableUtils;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AddCloneTask {

    private final String sourceClusterId;
    private final String targetClusterId;
    private final String cloneClusterInstanceType;
    private final int replicaCount;
    private final String engineVersion;
    private final Supplier<AmazonNeptune> amazonNeptuneClientSupplier;
    private final String cloneCorrelationId;

    public AddCloneTask(String sourceClusterId,
                        String targetClusterId,
                        String cloneClusterInstanceType,
                        int replicaCount,
                        String engineVersion,
                        Supplier<AmazonNeptune> amazonNeptuneClientSupplier,
                        String cloneCorrelationId) {
        this.sourceClusterId = sourceClusterId;
        this.targetClusterId = targetClusterId;
        this.cloneClusterInstanceType = cloneClusterInstanceType;
        this.replicaCount = replicaCount;
        this.engineVersion = engineVersion;
        this.amazonNeptuneClientSupplier = amazonNeptuneClientSupplier;
        this.cloneCorrelationId = cloneCorrelationId;
    }

    public NeptuneClusterMetadata execute() {
        return Timer.timedActivity(
                "cloning cluster",
                (Activity.Callable<NeptuneClusterMetadata>) this::cloneCluster);
    }

    private NeptuneClusterMetadata cloneCluster() {

        System.err.println("Cloning cluster " + sourceClusterId + "...");
        System.err.println();

        NeptuneClusterMetadata sourceClusterMetadata =
                NeptuneClusterMetadata.createFromClusterId(sourceClusterId, amazonNeptuneClientSupplier);

        InstanceType instanceType = StringUtils.isEmpty(cloneClusterInstanceType) ?
                InstanceType.parse(sourceClusterMetadata.instanceMetadataFor(sourceClusterMetadata.primary()).instanceType()) :
                InstanceType.parse(cloneClusterInstanceType);

        System.err.println(String.format("Source clusterId           : %s", sourceClusterId));
        System.err.println(String.format("Target clusterId           : %s", targetClusterId));
        System.err.println(String.format("Target instance type       : %s", instanceType));

        AmazonNeptune neptune = amazonNeptuneClientSupplier.get();

        DBClusterParameterGroup dbClusterParameterGroup = Timer.timedActivity(
                "creating DB cluster parameter group",
                (Activity.Callable<DBClusterParameterGroup>) () ->
                        createDbClusterParameterGroup(sourceClusterMetadata, neptune));

        DBParameterGroup dbParameterGroup = Timer.timedActivity(
                "creating parameter groups",
                (Activity.Callable<DBParameterGroup>) () -> createDbParameterGroup(sourceClusterMetadata, neptune));


        DBCluster targetDbCluster = Timer.timedActivity(
                "creating target cluster",
                (Activity.Callable<DBCluster>) () ->
                        createCluster(sourceClusterMetadata, neptune, dbClusterParameterGroup));

        Timer.timedActivity("creating primary", (Activity.Runnable) () ->
                createInstance("primary",
                        neptune,
                        sourceClusterMetadata,
                        instanceType,
                        dbParameterGroup,
                        targetDbCluster));

        if (replicaCount > 0) {

            Timer.timedActivity("creating replicas", (Activity.Runnable) () ->
                    createReplicas(sourceClusterMetadata, instanceType, neptune, dbParameterGroup, targetDbCluster));
        }

        neptune.shutdown();

        return NeptuneClusterMetadata.createFromClusterId(targetClusterId, amazonNeptuneClientSupplier);
    }

    private void createReplicas(NeptuneClusterMetadata sourceClusterMetadata,
                                InstanceType instanceType,
                                AmazonNeptune neptune,
                                DBParameterGroup dbParameterGroup,
                                DBCluster targetDbCluster) {

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

    private DBCluster createCluster(NeptuneClusterMetadata sourceClusterMetadata,
                                    AmazonNeptune neptune,
                                    DBClusterParameterGroup
                                            dbClusterParameterGroup) {

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
                .withTags(getTags(sourceClusterMetadata.clusterId()));

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

        return targetDbCluster;
    }

    private Collection<Tag> getTags(String sourceClusterId) {
        Collection<Tag> tags = new ArrayList<>();
        tags.add(new Tag()
                .withKey("source")
                .withValue(sourceClusterId));
        tags.add(new Tag()
                .withKey("application")
                .withValue(NeptuneClusterMetadata.NEPTUNE_EXPORT_APPLICATION_TAG));

        if (StringUtils.isNotEmpty(cloneCorrelationId)) {
            tags.add(new Tag()
                    .withKey(NeptuneClusterMetadata.NEPTUNE_EXPORT_CORRELATION_ID_KEY)
                    .withValue(cloneCorrelationId));
        }

        return tags;
    }

    private DBParameterGroup createDbParameterGroup(NeptuneClusterMetadata sourceClusterMetadata,
                                                    AmazonNeptune neptune) {

        DBParameterGroup dbParameterGroup;

        dbParameterGroup = neptune.createDBParameterGroup(
                new CreateDBParameterGroupRequest()
                        .withDBParameterGroupName(String.format("%s-db-params", targetClusterId))
                        .withDescription(String.format("%s DB Parameter Group", targetClusterId))
                        .withDBParameterGroupFamily("neptune1")
                        .withTags(getTags(sourceClusterMetadata.clusterId())));

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

        return dbParameterGroup;
    }

    private DBClusterParameterGroup createDbClusterParameterGroup(NeptuneClusterMetadata sourceClusterMetadata,
                                                                  AmazonNeptune neptune) {
        DBClusterParameterGroup dbClusterParameterGroup;

        dbClusterParameterGroup = neptune.createDBClusterParameterGroup(
                new CreateDBClusterParameterGroupRequest()
                        .withDBClusterParameterGroupName(String.format("%s-db-cluster-params", targetClusterId))
                        .withDescription(String.format("%s DB Cluster Parameter Group", targetClusterId))
                        .withDBParameterGroupFamily("neptune1")
                        .withTags(getTags(sourceClusterMetadata.clusterId())));

        String neptuneStreamsParameterValue = sourceClusterMetadata.isStreamEnabled() ? "1" : "0";

        try {

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
                                    .withApplyMethod(ApplyMethod.PendingReboot),
                            new Parameter()
                                    .withParameterName("neptune_streams")
                                    .withParameterValue(neptuneStreamsParameterValue)
                                    .withApplyMethod(ApplyMethod.PendingReboot)));
        } catch (AmazonNeptuneException e) {
            neptune.modifyDBClusterParameterGroup(new ModifyDBClusterParameterGroupRequest()
                    .withDBClusterParameterGroupName(dbClusterParameterGroup.getDBClusterParameterGroupName())
                    .withParameters(
                            new Parameter()
                                    .withParameterName("neptune_query_timeout")
                                    .withParameterValue("2147483647")
                                    .withApplyMethod(ApplyMethod.PendingReboot),
                            new Parameter()
                                    .withParameterName("neptune_streams")
                                    .withParameterValue(neptuneStreamsParameterValue)
                                    .withApplyMethod(ApplyMethod.PendingReboot)));
        }

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

        return dbClusterParameterGroup;
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
                .withTags(getTags(sourceClusterMetadata.clusterId()));

        if (StringUtils.isNotEmpty(engineVersion)) {
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
