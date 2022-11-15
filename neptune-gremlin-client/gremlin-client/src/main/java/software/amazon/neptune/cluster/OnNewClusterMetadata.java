package software.amazon.neptune.cluster;

import java.util.Collection;
import java.util.Map;

public interface OnNewClusterMetadata {
    void apply(NeptuneClusterMetadata neptuneClusterMetadata);
}
