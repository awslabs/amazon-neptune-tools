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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.cluster.*;
import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;

import java.util.function.Supplier;

public class CloneClusterModule {

    @Option(name = {"--clone-cluster"}, description = "Clone an Amazon Neptune cluster.")
    @Once
    private boolean cloneCluster = false;

    @Option(name = {"--clone-cluster-instance-type"}, description = "Instance type for cloned cluster (by default neptune-export will use the same instance type as the source cluster).")
    @Once
    @AllowedValues(allowedValues = {
            "db.r4.large",
            "db.r4.xlarge",
            "db.r4.2xlarge",
            "db.r4.4xlarge",
            "db.r4.8xlarge",
            "db.r5.large",
            "db.r5.xlarge",
            "db.r5.2xlarge",
            "db.r5.4xlarge",
            "db.r5.8xlarge",
            "db.r5.12xlarge",
            "db.t3.medium",
            "r4.large",
            "r4.xlarge",
            "r4.2xlarge",
            "r4.4xlarge",
            "r4.8xlarge",
            "r5.large",
            "r5.xlarge",
            "r5.2xlarge",
            "r5.4xlarge",
            "r5.8xlarge",
            "r5.12xlarge",
            "t3.medium"})
    private String cloneClusterInstanceType;

    @Option(name = {"--clone-cluster-replica-count"}, description = "Number of read replicas to add to the cloned cluster (default, 0).")
    @Once
    @IntegerRange(min = 0, minInclusive = true, max = 15, maxInclusive = true)
    private int replicaCount = 0;

    @Option(name = {"--clone-cluster-max-concurrency"}, description = "Limits concurrency when exporting from cloned cluster (default, no limit).", hidden = true)
    @Once
    private int maxConcurrency = -1;

    @Option(name = {"--clone-cluster-engine-version"}, description = "Cloned cluster Neptune engine version (default, latest).", hidden = true)
    @Once
    private String engineVersion;

    @Option(name = {"--clone-cluster-correlation-id"}, description = "Correlation ID to be added to a correlation-id tag on the cloned cluster.")
    @Once
    private String cloneCorrelationId;

    private final Supplier<AmazonNeptune> amazonNeptuneClientSupplier;

    public CloneClusterModule(Supplier<AmazonNeptune> amazonNeptuneClientSupplier) {
        this.amazonNeptuneClientSupplier = amazonNeptuneClientSupplier;
    }

    public Cluster cloneCluster(ConnectionConfig connectionConfig, ConcurrencyConfig concurrencyConfig, FeatureToggles featureToggles) throws Exception {
        if (cloneCluster){
            if (featureToggles.containsFeature(FeatureToggle.Simulate_Cloned_Cluster)){
                return new SimulatedCloneCluster(amazonNeptuneClientSupplier).cloneCluster(connectionConfig, concurrencyConfig);
            } else {

                CloneCluster command = new CloneCluster(
                        cloneClusterInstanceType,
                        replicaCount,
                        maxConcurrency,
                        engineVersion,
                        amazonNeptuneClientSupplier,
                        cloneCorrelationId);
                return command.cloneCluster(connectionConfig, concurrencyConfig);
            }
        } else {
            return new DoNotCloneCluster(amazonNeptuneClientSupplier).cloneCluster(connectionConfig, concurrencyConfig);
        }
    }
}
