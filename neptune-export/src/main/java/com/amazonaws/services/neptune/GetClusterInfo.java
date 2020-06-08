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

import com.amazonaws.services.neptune.cluster.NeptuneClusterMetadata;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import org.apache.commons.lang.StringUtils;

@Command(name = "nei", description = "neptune-export cluster info", hidden = true)
public class GetClusterInfo implements Runnable {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint", title = "endpoint")
    @Once
    private String endpoint;

    @Option(name = {"--cluster-id"}, description = "Neptune cluster ID", title = "clusterId")
    @Once
    private String clusterId;

    @Override
    public void run() {
        try {

            if (StringUtils.isEmpty(endpoint) && StringUtils.isEmpty(clusterId)) {
                throw new IllegalArgumentException("You must supply an endpoint or cluster ID");
            }

            NeptuneClusterMetadata metadata = StringUtils.isNotEmpty(clusterId) ?
                    NeptuneClusterMetadata.createFromClusterId(clusterId) :
                    NeptuneClusterMetadata.createFromClusterId(NeptuneClusterMetadata.clusterIdFromEndpoint(endpoint));

            printClusterDetails(metadata);

        } catch (Exception e) {
            System.err.println("An error occurred while creating Neptune cluster info:");
            e.printStackTrace();
        }
    }

    public static void printClusterDetails(NeptuneClusterMetadata metadata){
        System.err.println();

        System.err.println("Cluster ID              : " + metadata.clusterId());
        System.err.println("Port                    : " + metadata.port());
        System.err.println("IAM DB Auth             : " + metadata.isIAMDatabaseAuthenticationEnabled());
        System.err.println("Cluster parameter group : " + metadata.dbClusterParameterGroupName());
        System.err.println("Subnet group            : " + metadata.dbSubnetGroupName());
        System.err.println("Security group IDs      : " + String.join(", ", metadata.vpcSecurityGroupIds()));
        System.err.println("Instance endpoints      : " + String.join(", ", metadata.endpoints()));

        NeptuneClusterMetadata.NeptuneInstanceMetadata primary = metadata.instanceMetadataFor(metadata.primary());
        System.err.println();
        System.err.println("Primary");
        System.err.println("  Instance ID              : " + metadata.primary());
        System.err.println("  Instance type            : " + primary.instanceType());
        System.err.println("  Endpoint                 : " + primary.endpoint().getAddress());
        System.err.println("  Database parameter group : " + primary.dbParameterGroupName());

        if (!metadata.replicas().isEmpty()) {
            for (String replicaId : metadata.replicas()) {
                NeptuneClusterMetadata.NeptuneInstanceMetadata replica = metadata.instanceMetadataFor(replicaId);
                System.err.println();
                System.err.println("Replica");
                System.err.println("  Instance ID              : " + replicaId);
                System.err.println("  Instance type            : " + replica.instanceType());
                System.err.println("  Endpoint                 : " + replica.endpoint().getAddress());
                System.err.println("  Database parameter group : " + replica.dbParameterGroupName());
            }
        }
    }
}
