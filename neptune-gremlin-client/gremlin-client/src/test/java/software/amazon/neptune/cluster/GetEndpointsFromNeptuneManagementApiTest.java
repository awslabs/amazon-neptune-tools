package software.amazon.neptune.cluster;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GetEndpointsFromNeptuneManagementApiTest {
    @Test
    public void implementsClusterMetadataFetchStrategy(){
        ClusterEndpointsFetchStrategy command =
                new GetEndpointsFromNeptuneManagementApi("clusterId", Arrays.asList(EndpointsType.ClusterEndpoint));
        Assert.assertTrue(ClusterMetadataFetchStrategy.class.isAssignableFrom(command.getClass()));
    }
}