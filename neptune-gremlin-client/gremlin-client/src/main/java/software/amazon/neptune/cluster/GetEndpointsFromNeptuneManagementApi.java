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

import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.*;

import java.util.*;
import java.util.stream.Collectors;

public class GetEndpointsFromNeptuneManagementApi implements ClusterEndpointsFetchStrategy {

    private static final Map<String, Map<String, String>> instanceTags = new HashMap<>();

    private final String clusterId;
    private final Collection<EndpointsSelector> selectors;

    public GetEndpointsFromNeptuneManagementApi(String clusterId, Collection<EndpointsSelector> selectors) {
        this.clusterId = clusterId;
        this.selectors = selectors;
    }

    @Override
    public Map<EndpointsSelector, Collection<String>> getAddresses() {
        AmazonNeptune neptune = AmazonNeptuneClientBuilder.defaultClient();

        DescribeDBClustersResult describeDBClustersResult = neptune
                .describeDBClusters(new DescribeDBClustersRequest().withDBClusterIdentifier(clusterId));

        if (describeDBClustersResult.getDBClusters().isEmpty()) {
            throw new IllegalStateException(String.format("Unable to find cluster %s", clusterId));
        }

        DBCluster dbCluster = describeDBClustersResult.getDBClusters().get(0);

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

        Map<String, NeptuneInstanceProperties> instances = new HashMap<>();
        describeDBInstancesResult.getDBInstances()
                .forEach(c -> instances.put(
                        c.getDBInstanceIdentifier(),
                        new NeptuneInstanceProperties(
                                c.getDBInstanceIdentifier(),
                                c.getEndpoint().getAddress(),
                                c.getDBInstanceStatus(),
                                c.getAvailabilityZone(),
                                c.getDBInstanceClass(),
                                getTags(c.getDBInstanceArn(), neptune))
                ));

        neptune.shutdown();

        Map<EndpointsSelector, Collection<String>> results = new HashMap<>();

        for (EndpointsSelector selector : selectors) {
            results.put(selector, selector.getEndpoints(primary, replicas, instances));
        }

        return results;
    }

    private Map<String, String> getTags(String dbInstanceArn, AmazonNeptune neptune) {
        if (instanceTags.containsKey(dbInstanceArn)){
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
