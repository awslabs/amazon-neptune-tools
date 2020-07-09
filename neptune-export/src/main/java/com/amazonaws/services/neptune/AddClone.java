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

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.cluster.AddCloneTask;
import com.amazonaws.services.neptune.cluster.NeptuneClusterMetadata;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;

import java.util.UUID;

@Command(name = "add-clone", description = "Clone an Amazon Neptune database cluster")
public class AddClone implements Runnable {

    @Option(name = {"--source-cluster-id"}, description = "Cluster ID of the source Amazon Neptune database cluster")
    @Required
    @Once
    private String sourceClusterId;

    @Option(name = {"--clone-cluster-id"}, description = "Cluster ID of the cloned Amazon Neptune database cluster")
    @Once
    private String targetClusterId = String.format("neptune-export-cluster-%s", UUID.randomUUID().toString().substring(0, 5));

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

    @Option(name = {"--clone-cluster-engine-version"}, description = "Cloned cluster Neptune engine version (default, latest)", hidden = true)
    @Once
    private String engineVersion;

    @Override
    public void run() {
        try {
            AddCloneTask addCloneTask = new AddCloneTask(sourceClusterId, targetClusterId, cloneClusterInstanceType, replicaCount, engineVersion);
            NeptuneClusterMetadata clusterMetadata = addCloneTask.execute();

            GetClusterInfo.printClusterDetails(clusterMetadata);

            System.out.println(clusterMetadata.clusterId());

        } catch (Exception e) {
            System.err.println("An error occurred while cloning an Amazon Neptune database cluster:");
            e.printStackTrace();
        }
    }
}
