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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.EnvironmentVariableUtils;
import software.amazon.utils.RegionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GetEndpointsFromNeptuneManagementApi implements ClusterEndpointsFetchStrategy {

    private static final Logger logger = LoggerFactory.getLogger(GetEndpointsFromNeptuneManagementApi.class);

    private static final Map<String, Map<String, String>> instanceTags = new HashMap<>();

    private final String clusterId;
    private final String region;
    private final Collection<EndpointsSelector> selectors;
    private final AtomicReference<Map<EndpointsSelector, Collection<String>>> previousResults = new AtomicReference<>();

    public GetEndpointsFromNeptuneManagementApi(String clusterId, Collection<EndpointsSelector> selectors) {
        this(clusterId, RegionUtils.getCurrentRegionName(), selectors);
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId, String region, Collection<EndpointsSelector> selectors) {
        this.clusterId = clusterId;
        this.region = region;
        this.selectors = selectors;
    }

    @Override
    public Map<EndpointsSelector, Collection<String>> getAddresses() {

        try {
            AmazonNeptune neptune = StringUtils.isEmpty(region) ?
                    AmazonNeptuneClientBuilder.defaultClient() :
                    AmazonNeptuneClientBuilder.standard().withRegion(region).build();

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

            Collection<NeptuneInstanceProperties> instances = new ArrayList<>();
            describeDBInstancesResult.getDBInstances()
                    .forEach(c -> {
                                String role = "unknown";
                                if (primary.equals(c.getDBInstanceIdentifier())) {
                                    role = "writer";
                                }
                                if (replicas.contains(c.getDBInstanceIdentifier())) {
                                    role = "reader";
                                }
                                instances.add(
                                        new NeptuneInstanceProperties(
                                                c.getDBInstanceIdentifier(),
                                                role,
                                                c.getEndpoint().getAddress(),
                                                c.getDBInstanceStatus(),
                                                c.getAvailabilityZone(),
                                                c.getDBInstanceClass(),
                                                getTags(c.getDBInstanceArn(), neptune)));
                            }
                    );

            neptune.shutdown();

            Map<EndpointsSelector, Collection<String>> results = new HashMap<>();

            for (EndpointsSelector selector : selectors) {
                results.put(selector, selector.getEndpoints(clusterEndpoint, readerEndpoint, instances));
            }

            previousResults.set(results);

            return results;

        } catch (AmazonNeptuneException e) {
            if (e.getErrorCode().equals("Throttling")) {
                Map<EndpointsSelector, Collection<String>> results = previousResults.get();
                if (results != null) {
                    logger.warn("Calls to the Neptune Management API are being throttled. Reduce the refresh rate and stagger refresh agent requests, or use a NeptuneEndpointsInfoLambda proxy.");
                    return results;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
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
