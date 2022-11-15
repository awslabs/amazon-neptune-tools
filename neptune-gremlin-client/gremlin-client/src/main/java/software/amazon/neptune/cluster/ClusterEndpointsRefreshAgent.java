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

import org.apache.tinkerpop.gremlin.driver.IamAuthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterEndpointsRefreshAgent implements AutoCloseable {

    public static ClusterEndpointsRefreshAgent lambdaProxy(
            EndpointsType endpointsType,
            String lambdaName){
        return lambdaProxy(endpointsType, lambdaName, RegionUtils.getCurrentRegionName());
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(
            EndpointsType endpointsType,
            String lambdaName,
            String region){
        return lambdaProxy(endpointsType, lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(
            EndpointsType endpointsType,
            String lambdaName,
            String region,
            String iamProfile){
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(endpointsType, lambdaName, region, iamProfile));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(
            EndpointsSelector endpointsSelector,
            String lambdaName){
        return lambdaProxy(endpointsSelector, lambdaName, RegionUtils.getCurrentRegionName());
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(
            EndpointsSelector endpointsSelector,
            String lambdaName,
            String region){
        return lambdaProxy(endpointsSelector, lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(
            EndpointsSelector endpointsSelector,
            String lambdaName,
            String region,
            String iamProfile){
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(endpointsSelector, lambdaName, region, iamProfile));
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterEndpointsRefreshAgent.class);

    private final ClusterEndpointsFetchStrategy clusterEndpointsFetchStrategy;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public ClusterEndpointsRefreshAgent(ClusterEndpointsFetchStrategy clusterEndpointsFetchStrategy) {
        this.clusterEndpointsFetchStrategy = clusterEndpointsFetchStrategy;
    }

    public ClusterEndpointsRefreshAgent(String clusterId, EndpointsSelector... selectors) {
        this(new GetEndpointsFromNeptuneManagementApi(clusterId, Arrays.asList(selectors)));
    }

    public void startPollingNeptuneAPI(OnNewAddresses onNewAddresses,
                                       long delay,
                                       TimeUnit timeUnit) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try{
                Map<EndpointsSelector, Collection<String>> addresses = getAddresses();
                logger.info("New addresses: {}", addresses);
                onNewAddresses.apply(addresses);
            } catch (Exception e){
                logger.error("Error while getting addresses", e);
            }

        }, delay, delay, timeUnit);
    }

    public void startPollingNeptuneAPI(OnNewClusterMetadata onNewClusterMetadata,
                                       long delay,
                                       TimeUnit timeUnit) {

        if (!ClusterMetadataFetchStrategy.class.isAssignableFrom(clusterEndpointsFetchStrategy.getClass())){
            throw new IllegalStateException("Fetch strategy does not implement ClusterMetadataFetchStrategy: "
                    + clusterEndpointsFetchStrategy.getClass().getSimpleName());
        }

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try{
                NeptuneClusterMetadata clusterMetadata = getClusterMetadata();
                logger.info("New cluster metadata: {}", clusterMetadata);
                onNewClusterMetadata.apply(clusterMetadata);
            } catch (Exception e){
                logger.error("Error while getting cluster metadata", e);
            }

        }, delay, delay, timeUnit);
    }

    public void stop() {
        scheduledExecutorService.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    public Map<EndpointsSelector, Collection<String>> getAddresses() {
        return clusterEndpointsFetchStrategy.getAddresses();
    }

    public NeptuneClusterMetadata getClusterMetadata() {
        return ((ClusterMetadataFetchStrategy)clusterEndpointsFetchStrategy).getClusterMetadata();
    }
}
