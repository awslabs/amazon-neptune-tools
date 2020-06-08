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
import com.amazonaws.services.neptune.model.*;

import java.util.*;
import java.util.stream.Collectors;

public class NeptuneClusterMetadata {

    public static final String NEPTUNE_EXPORT_APPLICATION_TAG = "neptune-export";

    public static String clusterIdFromEndpoint(String endpoint) {
        int index = endpoint.indexOf(".");
        return endpoint.substring(0, index);
    }

    public static NeptuneClusterMetadata createFromClusterId(String clusterId) {
        AmazonNeptune neptune = AmazonNeptuneClientBuilder.defaultClient();

        DescribeDBClustersResult describeDBClustersResult = neptune
                .describeDBClusters(new DescribeDBClustersRequest().withDBClusterIdentifier(clusterId));

        if (describeDBClustersResult.getDBClusters().isEmpty()) {
            throw new IllegalArgumentException(String.format("Unable to find cluster %s", clusterId));
        }

        DBCluster dbCluster = describeDBClustersResult.getDBClusters().get(0);

        List<Tag> tags = neptune.listTagsForResource(
                new ListTagsForResourceRequest()
                        .withResourceName(dbCluster.getDBClusterArn())).getTagList();

        Map<String, String> clusterTags = new HashMap<>();
        tags.forEach(t -> clusterTags.put(t.getKey(), t.getValue()));

        boolean isIAMDatabaseAuthenticationEnabled = dbCluster.isIAMDatabaseAuthenticationEnabled();
        Integer port = dbCluster.getPort();
        String dbClusterParameterGroup = dbCluster.getDBClusterParameterGroup();
        String dbSubnetGroup = dbCluster.getDBSubnetGroup();
        List<VpcSecurityGroupMembership> vpcSecurityGroups = dbCluster.getVpcSecurityGroups();
        List<String> vpcSecurityGroupIds = vpcSecurityGroups.stream()
                .map(VpcSecurityGroupMembership::getVpcSecurityGroupId)
                .collect(Collectors.toList());

        List<DBClusterMember> dbClusterMembers = dbCluster.getDBClusterMembers();
        Optional<DBClusterMember> clusterWriter = dbClusterMembers.stream()
                .filter(DBClusterMember::isClusterWriter)
                .findFirst();

        String primary = clusterWriter.map(DBClusterMember::getDBInstanceIdentifier).orElse("");
        List<String> replicas = dbClusterMembers.stream()
                .filter(dbClusterMember -> !dbClusterMember.isClusterWriter())
                .map(DBClusterMember::getDBInstanceIdentifier)
                .collect(Collectors.toList());

        DescribeDBInstancesRequest describeDBInstancesRequest = new DescribeDBInstancesRequest()
                .withFilters(Collections.singletonList(
                        new Filter()
                                .withName("db-cluster-id")
                                .withValues(dbCluster.getDBClusterIdentifier())));

        DescribeDBInstancesResult describeDBInstancesResult = neptune
                .describeDBInstances(describeDBInstancesRequest);

        Map<String, NeptuneInstanceMetadata> instanceTypes = new HashMap<>();
        describeDBInstancesResult.getDBInstances()
                .forEach(c -> instanceTypes.put(
                        c.getDBInstanceIdentifier(),
                        new NeptuneInstanceMetadata(
                                c.getDBInstanceClass(),
                                c.getDBParameterGroups().get(0).getDBParameterGroupName(),
                                c.getEndpoint())
                ));

        neptune.shutdown();

        return new NeptuneClusterMetadata(clusterId,
                port,
                dbClusterParameterGroup,
                isIAMDatabaseAuthenticationEnabled,
                dbSubnetGroup,
                vpcSecurityGroupIds,
                primary,
                replicas,
                instanceTypes,
                clusterTags);
    }

    private final String clusterId;

    private final int port;
    private final String dbClusterParameterGroupName;
    private final Boolean isIAMDatabaseAuthenticationEnabled;
    private final String dbSubnetGroupName;
    private final Collection<String> vpcSecurityGroupIds;
    private final String primary;
    private final Collection<String> replicas;
    private final Map<String, NeptuneInstanceMetadata> instanceMetadata;
    private final Map<String, String> clusterTags;

    private NeptuneClusterMetadata(String clusterId,
                                   int port,
                                   String dbClusterParameterGroupName,
                                   Boolean isIAMDatabaseAuthenticationEnabled,
                                   String dbSubnetGroupName,
                                   List<String> vpcSecurityGroupIds,
                                   String primary,
                                   Collection<String> replicas,
                                   Map<String, NeptuneInstanceMetadata> instanceMetadata,
                                   Map<String, String> clusterTags) {
        this.clusterId = clusterId;
        this.port = port;
        this.dbClusterParameterGroupName = dbClusterParameterGroupName;

        this.isIAMDatabaseAuthenticationEnabled = isIAMDatabaseAuthenticationEnabled;
        this.dbSubnetGroupName = dbSubnetGroupName;
        this.vpcSecurityGroupIds = vpcSecurityGroupIds;
        this.primary = primary;
        this.replicas = replicas;
        this.instanceMetadata = instanceMetadata;
        this.clusterTags = clusterTags;
    }

    public String clusterId() {
        return clusterId;
    }

    public int port() {
        return port;
    }

    public String dbClusterParameterGroupName() {
        return dbClusterParameterGroupName;
    }

    public Boolean isIAMDatabaseAuthenticationEnabled() {
        return isIAMDatabaseAuthenticationEnabled;
    }

    public String dbSubnetGroupName() {
        return dbSubnetGroupName;
    }

    public Collection<String> vpcSecurityGroupIds() {
        return vpcSecurityGroupIds;
    }

    public String primary() {
        return primary;
    }

    public Collection<String> replicas() {
        return replicas;
    }

    public NeptuneInstanceMetadata instanceMetadataFor(String key) {
        return instanceMetadata.get(key);
    }

    public List<String> endpoints() {
        return instanceMetadata.values().stream().map(i -> i.endpoint().getAddress()).collect(Collectors.toList());
    }

    public boolean isTaggedWithNeptuneExport(){
        return clusterTags.containsKey("application") &&
                clusterTags.get("application").equalsIgnoreCase(NEPTUNE_EXPORT_APPLICATION_TAG);
    }

    public static class NeptuneInstanceMetadata {
        private final String instanceType;
        private final String dbParameterGroupName;
        private final Endpoint endpoint;

        public NeptuneInstanceMetadata(String instanceType, String dbParameterGroupName, Endpoint endpoint) {
            this.instanceType = instanceType;
            this.dbParameterGroupName = dbParameterGroupName;
            this.endpoint = endpoint;
        }

        public String instanceType() {
            return instanceType;
        }

        public String dbParameterGroupName() {
            return dbParameterGroupName;
        }

        public Endpoint endpoint() {
            return endpoint;
        }
    }

}
