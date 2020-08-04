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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClusterEndpointsRefreshAgent implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterEndpointsRefreshAgent.class);
    private static final Map<String, Map<String, String>> instanceTags = new HashMap<>();

    public interface OnNewAddresses {
        void apply(Collection<String> addresses);
    }

    public enum EndpointsType implements EndpointsSelector {
        All {
            @Override
            public Collection<String> getEndpoints(String primaryId,
                                                   Collection<String> replicaIds,
                                                   Map<String, NeptuneInstanceProperties> instances) {
                return instances.values().stream()
                        .filter(NeptuneInstanceProperties::isAvailable)
                        .map(NeptuneInstanceProperties::getEndpoint)
                        .collect(Collectors.toList());
            }
        },
        Primary {
            @Override
            public Collection<String> getEndpoints(String primaryId,
                                                   Collection<String> replicaIds,
                                                   Map<String, NeptuneInstanceProperties> instances) {
                return instances.values().stream()
                        .filter(i -> primaryId.equals(i.getInstanceId()))
                        .filter(NeptuneInstanceProperties::isAvailable)
                        .map(NeptuneInstanceProperties::getEndpoint)
                        .collect(Collectors.toList());
            }
        },
        ReadReplicas {
            @Override
            public Collection<String> getEndpoints(String primaryId,
                                                   Collection<String> replicaIds,
                                                   Map<String, NeptuneInstanceProperties> instances) {

                if (replicaIds.isEmpty()) {
                    return Collections.singleton(instances.get(primaryId).getEndpoint());
                }

                return instances.values().stream()
                        .filter(i -> replicaIds.contains(i.getInstanceId()))
                        .filter(NeptuneInstanceProperties::isAvailable)
                        .map(NeptuneInstanceProperties::getEndpoint)
                        .collect(Collectors.toList());
            }
        };
    }

    private final String clusterId;
    private final EndpointsSelector selector;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public ClusterEndpointsRefreshAgent(String clusterId, EndpointsSelector selector) {
        this.clusterId = clusterId;
        this.selector = selector;
    }

    public void startPollingNeptuneAPI(OnNewAddresses onNewAddresses,
                                       long delay,
                                       TimeUnit timeUnit) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            Collection<String> addresses = getAddresses();
            logger.info("New addresses: {}", addresses);
            onNewAddresses.apply(addresses);
        }, delay, delay, timeUnit);
    }

    public void stop() {
        scheduledExecutorService.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    public Collection<String> getAddresses() {
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

        return selector.getEndpoints(primary, replicas, instances);
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
