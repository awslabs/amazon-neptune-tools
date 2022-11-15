package software.amazon.neptune.cluster;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class NeptuneClusterMetadataTest {

    @Test
    public void serializeAndDeserializeClusterMetadata() throws IOException {
        HashMap<String, String> tags = new HashMap<>();
        tags.put("name", "my-writer");
        tags.put("app", "analytics");

        NeptuneInstanceMetadata instance1 = new NeptuneInstanceMetadata()
                .withInstanceId("instance-1")
                .withInstanceType("r5.large")
                .withAvailabilityZone("eu-west-1b")
                .withEndpoint("endpoint-1")
                .withStatus("available")
                .withRole("writer")
                .withTags(tags);

        NeptuneInstanceMetadata instance2 = new NeptuneInstanceMetadata()
                .withInstanceId("instance-2")
                .withInstanceType("r5.medium")
                .withAvailabilityZone("eu-west-1a")
                .withEndpoint("endpoint-2")
                .withStatus("rebooting")
                .withRole("reader")
                .withTags(tags);

        NeptuneClusterMetadata neptuneClusterMetadata = new NeptuneClusterMetadata()
                .withClusterEndpoint("cluster-endpoint")
                .withReaderEndpoint("reader-endpoint")
                .withInstances(Arrays.asList(instance1, instance2));

        String json1 = neptuneClusterMetadata.toJsonString();
        NeptuneClusterMetadata cluster = NeptuneClusterMetadata.fromByeArray(json1.getBytes());
        String json2 = cluster.toJsonString();

        Assert.assertEquals(json2, json1);
    }

}