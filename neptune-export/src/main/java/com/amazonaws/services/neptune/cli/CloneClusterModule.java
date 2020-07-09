/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.neptune.cluster.ConnectionConfig;
import com.amazonaws.services.neptune.cluster.CloneCluster;
import com.amazonaws.services.neptune.cluster.ClusterStrategy;
import com.amazonaws.services.neptune.cluster.DoNotCloneCluster;
import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;

import java.util.UUID;

public class CloneClusterModule {

    @Option(name = {"--clone-cluster"}, description = "Clone Neptune cluster")
    @Once
    private boolean cloneCluster = false;

    @Option(name = {"--clone-cluster-instance-type"}, description = "Instance type for cloned cluster (by default neptune-export will use the same instance type as the source cluster)")
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
            "db.m5.large",
            "db.m5.xlarge",
            "db.m5.2xlarge",
            "db.m5.3xlarge",
            "db.m5.8xlarge",
            "db.m5.12xlarge",
            "db.m5.16xlarge",
            "db.m5.24xlarge",
            "db.t3.medium"})
    private String cloneClusterInstanceType;

    @Option(name = {"--clone-cluster-replica-count"}, description = "Number of read replicas to add to the cloned cluster (default, 0)")
    @Once
    @IntegerRange(min = 0, minInclusive = true, max = 15, maxInclusive = true)
    private int replicaCount = 0;

    @Option(name = {"--clone-cluster-max-concurrency"}, description = "Limits concurrency when exporting from cloned cluster (default, no limit)", hidden = true)
    @Once
    private int maxConcurrency = -1;

    @Option(name = {"--clone-cluster-engine-version"}, description = "Cloned cluster Neptune engine version (default, latest)", hidden = true)
    @Once
    private String engineVersion;

    public ClusterStrategy cloneCluster(ConnectionConfig connectionConfig, ConcurrencyConfig concurrencyConfig) throws Exception {
        if (cloneCluster){
            return new CloneCluster(cloneClusterInstanceType, replicaCount, maxConcurrency, engineVersion).cloneCluster(connectionConfig, concurrencyConfig);
        } else {
            return new DoNotCloneCluster().cloneCluster(connectionConfig, concurrencyConfig);
        }
    }
}
