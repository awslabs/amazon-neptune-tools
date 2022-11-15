package software.amazon.neptune.cluster;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class NeptuneClusterMetadata {

    public static NeptuneClusterMetadata fromByeArray(byte[] bytes) throws IOException {
        return new ObjectMapper().readerFor(NeptuneClusterMetadata.class).readValue(bytes);
    }

    private final Collection<NeptuneInstanceMetadata> instances = new ArrayList<>();
    private String clusterEndpoint;
    private String readerEndpoint;

    public NeptuneClusterMetadata(){

    }

    public void setClusterEndpoint(String clusterEndpoint) {
        this.clusterEndpoint = clusterEndpoint;
    }

    public void setReaderEndpoint(String readerEndpoint) {
        this.readerEndpoint = readerEndpoint;
    }

    public void setInstances(Collection<NeptuneInstanceMetadata> instances) {
        this.instances.clear();
        this.instances.addAll(instances);
    }

    public NeptuneClusterMetadata withClusterEndpoint(String clusterEndpoint) {
        setClusterEndpoint(clusterEndpoint);
        return this;
    }

    public NeptuneClusterMetadata withReaderEndpoint(String readerEndpoint) {
        setReaderEndpoint(readerEndpoint);
        return this;
    }

    public NeptuneClusterMetadata withInstances(Collection<NeptuneInstanceMetadata> instances) {
        setInstances(instances);
        return this;
    }


    public Collection<NeptuneInstanceMetadata> getInstances() {
        return instances;
    }

    public String getClusterEndpoint() {
        return clusterEndpoint;
    }

    public String getReaderEndpoint() {
        return readerEndpoint;
    }

    @Override
    public String toString() {
        return "NeptuneClusterMetadata{" +
                "instances=" + instances +
                ", clusterEndpoint='" + clusterEndpoint + '\'' +
                ", readerEndpoint='" + readerEndpoint + '\'' +
                '}';
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writerFor(this.getClass()).writeValueAsString(this);
    }
}
