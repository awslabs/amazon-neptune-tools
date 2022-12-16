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

import com.amazonaws.services.neptune.cluster.*;
import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;

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
            "db.r5.16xlarge",
            "db.r5.24xlarge",
            "db.r5d.large",
            "db.r5d.xlarge",
            "db.r5d.2xlarge",
            "db.r5d.4xlarge",
            "db.r5d.8xlarge",
            "db.r5d.12xlarge",
            "db.r5d.16xlarge",
            "db.r5d.24xlarge",
            "db.r6d.large",
            "db.r6d.xlarge",
            "db.r6d.2xlarge",
            "db.r6d.4xlarge",
            "db.r6d.8xlarge",
            "db.r6d.12xlarge",
            "db.r6d.16xlarge",
            "db.x2g.large",
            "db.x2g.xlarge",
            "db.x2g.2xlarge",
            "db.x2g.4xlarge",
            "db.x2g.8xlarge",
            "db.x2g.12xlarge",
            "db.x2g.16xlarge",
            "db.t3.medium",
            "db.t4g.medium",
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
            "r5.16xlarge",
            "r5.24xlarge",
            "r5d.large",
            "r5d.xlarge",
            "r5d.2xlarge",
            "r5d.4xlarge",
            "r5d.8xlarge",
            "r5d.12xlarge",
            "r5d.16xlarge",
            "r5d.24xlarge",
            "r6d.large",
            "r6d.xlarge",
            "r6d.2xlarge",
            "r6d.4xlarge",
            "r6d.8xlarge",
            "r6d.12xlarge",
            "r6d.16xlarge",
            "x2g.large",
            "x2g.xlarge",
            "x2g.2xlarge",
            "x2g.4xlarge",
            "x2g.8xlarge",
            "x2g.12xlarge",
            "x2g.16xlarge",
            "t3.medium",
            "t4g.medium"})
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


    public CloneClusterModule() {
    }

    public Cluster cloneCluster(NeptuneClusterMetadata clusterMetadata,
                                ConnectionConfig connectionConfig,
                                ConcurrencyConfig concurrencyConfig,
                                FeatureToggles featureToggles) throws Exception {

        clusterMetadata.printDetails();

        if (cloneCluster) {
            if (featureToggles.containsFeature(FeatureToggle.Simulate_Cloned_Cluster)) {
                return new SimulatedCloneCluster(clusterMetadata).cloneCluster(connectionConfig, concurrencyConfig);
            } else {

                CloneCluster command = new CloneCluster(
                        clusterMetadata,
                        cloneClusterInstanceType,
                        replicaCount,
                        maxConcurrency,
                        engineVersion,
                        cloneCorrelationId);
                return command.cloneCluster(connectionConfig, concurrencyConfig);
            }
        } else {
            return new DoNotCloneCluster(clusterMetadata).cloneCluster(connectionConfig, concurrencyConfig);
        }
    }
}
