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

package software.amazon.neptune.cluster;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.IamAuthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GetEndpointsFromNeptuneManagementApi implements
        ClusterEndpointsFetchStrategy,
        ClusterMetadataFetchStrategy {

    private static final Logger logger = LoggerFactory.getLogger(GetEndpointsFromNeptuneManagementApi.class);

    private static final Map<String, Map<String, String>> instanceTags = new HashMap<>();
    private final String clusterId;
    private final String region;
    private final String iamProfile;
    private final AWSCredentialsProvider credentials;
    private final Collection<EndpointsSelector> selectors;
    private final AtomicReference<Map<EndpointsSelector, Collection<String>>> previousResults = new AtomicReference<>();
    private final AtomicReference<NeptuneClusterMetadata> previousClusterMetadata = new AtomicReference<>();

    public GetEndpointsFromNeptuneManagementApi(String clusterId, Collection<EndpointsSelector> selectors) {
        this(clusterId, selectors, RegionUtils.getCurrentRegionName());
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                Collection<EndpointsSelector> selectors,
                                                String region) {
        this(clusterId, selectors, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                Collection<EndpointsSelector> selectors,
                                                String region,
                                                String iamProfile) {
        this(clusterId, selectors, region, iamProfile, null);
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                Collection<EndpointsSelector> selectors,
                                                String region,
                                                AWSCredentialsProvider credentials) {
        this(clusterId, selectors, region, IamAuthConfig.DEFAULT_PROFILE, credentials);
    }

    private GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                 Collection<EndpointsSelector> selectors,
                                                 String region,
                                                 String iamProfile,
                                                 AWSCredentialsProvider credentials) {
        this.clusterId = clusterId;
        this.selectors = selectors;
        this.region = region;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
    }

    @Override
    public NeptuneClusterMetadata getClusterMetadata() {
        try {
            AmazonNeptuneClientBuilder builder = AmazonNeptuneClientBuilder.standard();

            if (StringUtils.isNotEmpty(region)) {
                builder = builder.withRegion(region);
            }

            if (credentials != null) {
                builder = builder.withCredentials(credentials);
            } else if (!iamProfile.equals(IamAuthConfig.DEFAULT_PROFILE)) {
                builder = builder.withCredentials(new ProfileCredentialsProvider(iamProfile));
            }

            AmazonNeptune neptune = builder.build();

            DescribeDBClustersResult describeDBClustersResult = neptune
                    .describeDBClusters(new DescribeDBClustersRequest().withDBClusterIdentifier(clusterId));

            if (describeDBClustersResult.getDBClusters().isEmpty()) {
                throw new IllegalStateException(String.format("Unable to find cluster %s", clusterId));
            }

            DBCluster dbCluster = describeDBClustersResult.getDBClusters().get(0);

            String clusterEndpoint = dbCluster.getEndpoint();
            String readerEndpoint = dbCluster.getReaderEndpoint();

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

            Collection<NeptuneInstanceMetadata> instances = new ArrayList<>();
            describeDBInstancesResult.getDBInstances()
                    .forEach(c -> {
                                String role = "unknown";
                                if (primary.equals(c.getDBInstanceIdentifier())) {
                                    role = "writer";
                                }
                                if (replicas.contains(c.getDBInstanceIdentifier())) {
                                    role = "reader";
                                }
                                String address = c.getEndpoint() == null ? null : c.getEndpoint().getAddress();
                                instances.add(
                                        new NeptuneInstanceMetadata()
                                                .withInstanceId(c.getDBInstanceIdentifier())
                                                .withRole(role)
                                                .withEndpoint(address)
                                                .withStatus(c.getDBInstanceStatus())
                                                .withAvailabilityZone(c.getAvailabilityZone())
                                                .withInstanceType(c.getDBInstanceClass())
                                                .withTags(getTags(c.getDBInstanceArn(), neptune)));
                            }
                    );

            neptune.shutdown();

            return new NeptuneClusterMetadata()
                    .withInstances(instances)
                    .withClusterEndpoint(clusterEndpoint)
                    .withReaderEndpoint(readerEndpoint);

        } catch (AmazonNeptuneException e) {
            if (e.getErrorCode().equals("Throttling")) {
                logger.warn("Calls to the Neptune Management API are being throttled. Reduce the refresh rate and stagger refresh agent requests, or use a NeptuneEndpointsInfoLambda proxy.");
                NeptuneClusterMetadata previous = previousClusterMetadata.get();
                if (previous != null) {
                    return previous;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }

    }

    @Override
    public Map<EndpointsSelector, Collection<String>> getAddresses() {

        NeptuneClusterMetadata clusterMetadata = getClusterMetadata();

        Map<EndpointsSelector, Collection<String>> results = new HashMap<>();

        for (EndpointsSelector selector : selectors) {
            results.put(selector, selector.getEndpoints(
                    clusterMetadata.getClusterEndpoint(),
                    clusterMetadata.getReaderEndpoint(),
                    clusterMetadata.getInstances()));
        }

        return results;
    }

    private Map<String, String> getTags(String dbInstanceArn, AmazonNeptune neptune) {
        if (instanceTags.containsKey(dbInstanceArn)) {
            return instanceTags.get(dbInstanceArn);
        }

        List<Tag> tagList = neptune.listTagsForResource(
                new ListTagsForResourceRequest()
                        .withResourceName(dbInstanceArn)).getTagList();

        Map<String, String> tags = new HashMap<>();
        tagList.forEach(t -> tags.put(t.getKey(), t.getValue()));

        instanceTags.put(dbInstanceArn, tags);

        return tags;
    }
}
