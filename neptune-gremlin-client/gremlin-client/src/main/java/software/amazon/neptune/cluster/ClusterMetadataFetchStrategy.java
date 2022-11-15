package software.amazon.neptune.cluster;

public interface ClusterMetadataFetchStrategy {
    NeptuneClusterMetadata getClusterMetadata();
}
