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
import com.amazonaws.services.neptune.export.EndpointValidator;
import com.amazonaws.services.neptune.model.*;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class NeptuneClusterMetadata {

    public static final String NEPTUNE_EXPORT_APPLICATION_TAG = "neptune-export";
    public static final String NEPTUNE_EXPORT_CORRELATION_ID_KEY = "correlation-id";

    public static String clusterIdFromEndpoint(String endpoint) {
        int index = endpoint.indexOf(".");
        if (index < 0) {
            throw new IllegalArgumentException(String.format("Unable to identify cluster ID from endpoint '%s'. Use the clusterId export parameter instead.", endpoint));
        }
        return endpoint.substring(0, index);
    }

    public static NeptuneClusterMetadata createFromEndpoints(Collection<String> endpoints, Supplier<AmazonNeptune> amazonNeptuneClientSupplier) {
        AmazonNeptune neptune = amazonNeptuneClientSupplier.get();

        String paginationToken = null;

        do {
            DescribeDBClustersResult describeDBClustersResult = neptune
                    .describeDBClusters(new DescribeDBClustersRequest()
                            .withMarker(paginationToken)
                            .withFilters(new Filter().withName("engine").withValues("neptune")));

            paginationToken = describeDBClustersResult.getMarker();

            for (DBCluster dbCluster : describeDBClustersResult.getDBClusters()) {
                for (String endpoint : endpoints) {
                    String endpointValue = getEndpointValue(endpoint);
                    if (endpointValue.equals(getEndpointValue(dbCluster.getEndpoint()))){
                        return createFromClusterId(dbCluster.getDBClusterIdentifier(), amazonNeptuneClientSupplier);
                    } else if (endpointValue.equals(getEndpointValue(dbCluster.getReaderEndpoint()))){
                        return createFromClusterId(dbCluster.getDBClusterIdentifier(), amazonNeptuneClientSupplier);
                    }
                }
            }
        } while (paginationToken != null);

        paginationToken = null;

        do {

            DescribeDBInstancesResult describeDBInstancesResult = neptune.describeDBInstances(
                    new DescribeDBInstancesRequest()
                            .withMarker(paginationToken)
                            .withFilters(new Filter().withName("engine").withValues("neptune")));

            paginationToken = describeDBInstancesResult.getMarker();

            for (DBInstance dbInstance : describeDBInstancesResult.getDBInstances()) {
                for (String endpoint : endpoints) {
                    String endpointValue = getEndpointValue(endpoint);
                    if (endpointValue.equals(getEndpointValue(dbInstance.getEndpoint().getAddress()))){
                        return createFromClusterId(dbInstance.getDBClusterIdentifier(), amazonNeptuneClientSupplier);
                    }
                }
            }

        } while (paginationToken != null);

        throw new IllegalStateException(String.format("Unable to identify cluster ID from endpoints: %s", endpoints));

    }

    private static String getEndpointValue(String endpoint) {
        return EndpointValidator.validate(endpoint).toLowerCase();
    }

    public static NeptuneClusterMetadata createFromClusterId(String clusterId, Supplier<AmazonNeptune> amazonNeptuneClientSupplier) {

        AmazonNeptune neptune = amazonNeptuneClientSupplier.get();

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
        String engineVersion = dbCluster.getEngineVersion();

        String dbParameterGroupFamily;

        try {
            DescribeDBClusterParameterGroupsResult describeDBClusterParameterGroupsResult = neptune.describeDBClusterParameterGroups(
                    new DescribeDBClusterParameterGroupsRequest()
                            .withDBClusterParameterGroupName(dbClusterParameterGroup));

            Optional<DBClusterParameterGroup> parameterGroup = describeDBClusterParameterGroupsResult
                    .getDBClusterParameterGroups().stream().findFirst();

            dbParameterGroupFamily = parameterGroup.isPresent() ?
                    parameterGroup.get().getDBParameterGroupFamily() :
                    "neptune1";

        } catch (AmazonNeptuneException e) {

            // Older deployments of Neptune Export service may not have requisite permissions to
            // describe cluster parameter group, so we'll try and guess the group family.


            if (StringUtils.isNotEmpty(engineVersion) && engineVersion.contains(".")) {
                int v = Integer.parseInt(engineVersion.split("\\.")[1]);
                dbParameterGroupFamily = v > 1 ? "neptune1.2" : "neptune1";
            } else {
                dbParameterGroupFamily = "neptune1";
            }
        }

        DescribeDBClusterParametersResult describeDBClusterParametersResult = neptune.describeDBClusterParameters(
                new DescribeDBClusterParametersRequest()
                        .withDBClusterParameterGroupName(dbClusterParameterGroup));
        Optional<Parameter> neptuneStreamsParameter = describeDBClusterParametersResult.getParameters().stream()
                .filter(parameter -> parameter.getParameterName().equals("neptune_streams"))
                .findFirst();
        boolean isStreamEnabled = neptuneStreamsParameter.isPresent() &&
                neptuneStreamsParameter.get().getParameterValue().equals("1");

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
                engineVersion,
                dbClusterParameterGroup,
                dbParameterGroupFamily,
                isIAMDatabaseAuthenticationEnabled,
                isStreamEnabled,
                dbSubnetGroup,
                vpcSecurityGroupIds,
                primary,
                replicas,
                instanceTypes,
                clusterTags,
                amazonNeptuneClientSupplier);
    }

    private final String clusterId;
    private final int port;
    private final String engineVersion;
    private final String dbClusterParameterGroupName;
    private final String dbParameterGroupFamily;
    private final Boolean isIAMDatabaseAuthenticationEnabled;
    private final Boolean isStreamEnabled;
    private final String dbSubnetGroupName;
    private final Collection<String> vpcSecurityGroupIds;
    private final String primary;
    private final Collection<String> replicas;
    private final Map<String, NeptuneInstanceMetadata> instanceMetadata;
    private final Map<String, String> clusterTags;

    private final Supplier<AmazonNeptune> amazonNeptuneClientSupplier;

    private NeptuneClusterMetadata(String clusterId,
                                   int port,
                                   String engineVersion,
                                   String dbClusterParameterGroupName,
                                   String dbParameterGroupFamily,
                                   Boolean isIAMDatabaseAuthenticationEnabled,
                                   Boolean isStreamEnabled,
                                   String dbSubnetGroupName,
                                   List<String> vpcSecurityGroupIds,
                                   String primary,
                                   Collection<String> replicas,
                                   Map<String, NeptuneInstanceMetadata> instanceMetadata,
                                   Map<String, String> clusterTags,
                                   Supplier<AmazonNeptune> amazonNeptuneClientSupplier) {
        this.clusterId = clusterId;
        this.port = port;
        this.engineVersion = engineVersion;
        this.dbClusterParameterGroupName = dbClusterParameterGroupName;
        this.dbParameterGroupFamily = dbParameterGroupFamily;
        this.isIAMDatabaseAuthenticationEnabled = isIAMDatabaseAuthenticationEnabled;
        this.isStreamEnabled = isStreamEnabled;
        this.dbSubnetGroupName = dbSubnetGroupName;
        this.vpcSecurityGroupIds = vpcSecurityGroupIds;
        this.primary = primary;
        this.replicas = replicas;
        this.instanceMetadata = instanceMetadata;
        this.clusterTags = clusterTags;
        this.amazonNeptuneClientSupplier = amazonNeptuneClientSupplier;
    }

    public String clusterId() {
        return clusterId;
    }

    public int port() {
        return port;
    }

    public String engineVersion() {
        return engineVersion;
    }

    public String dbClusterParameterGroupName() {
        return dbClusterParameterGroupName;
    }

    public String dbParameterGroupFamily() {
        return dbParameterGroupFamily;
    }

    public Boolean isIAMDatabaseAuthenticationEnabled() {
        return isIAMDatabaseAuthenticationEnabled;
    }

    public Boolean isStreamEnabled() {
        return isStreamEnabled;
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

    public boolean isTaggedWithNeptuneExport() {
        return clusterTags.containsKey("application") &&
                clusterTags.get("application").equalsIgnoreCase(NEPTUNE_EXPORT_APPLICATION_TAG);
    }

    public Supplier<AmazonNeptune> clientSupplier() {
        return amazonNeptuneClientSupplier;
    }

    public void printDetails(){
        System.err.println("Cluster ID              : " + clusterId());
        System.err.println("Port                    : " + port());
        System.err.println("Engine                  : " + engineVersion());
        System.err.println("IAM DB Auth             : " + isIAMDatabaseAuthenticationEnabled());
        System.err.println("Streams enabled         : " + isStreamEnabled());
        System.err.println("Parameter group family  : " + dbParameterGroupFamily());
        System.err.println("Cluster parameter group : " + dbClusterParameterGroupName());
        System.err.println("Subnet group            : " + dbSubnetGroupName());
        System.err.println("Security group IDs      : " + String.join(", ", vpcSecurityGroupIds()));
        System.err.println("Instance endpoints      : " + String.join(", ", endpoints()));

        NeptuneClusterMetadata.NeptuneInstanceMetadata primary = instanceMetadataFor(primary());
        System.err.println();
        System.err.println("Primary");
        System.err.println("  Instance ID              : " + primary());
        System.err.println("  Instance type            : " + primary.instanceType());
        System.err.println("  Endpoint                 : " + primary.endpoint().getAddress());
        System.err.println("  Database parameter group : " + primary.dbParameterGroupName());

        if (!replicas().isEmpty()) {
            for (String replicaId : replicas()) {
                NeptuneClusterMetadata.NeptuneInstanceMetadata replica = instanceMetadataFor(replicaId);
                System.err.println();
                System.err.println("Replica");
                System.err.println("  Instance ID              : " + replicaId);
                System.err.println("  Instance type            : " + replica.instanceType());
                System.err.println("  Endpoint                 : " + replica.endpoint().getAddress());
                System.err.println("  Database parameter group : " + replica.dbParameterGroupName());
            }
        }
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
