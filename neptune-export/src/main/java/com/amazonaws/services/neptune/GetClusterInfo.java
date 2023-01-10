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

import com.amazonaws.services.neptune.cli.AwsCliModule;
import com.amazonaws.services.neptune.cluster.NeptuneClusterMetadata;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;

@Command(name = "nei", description = "neptune-export cluster info", hidden = true)
public class GetClusterInfo implements Runnable {

    @Inject
    private AwsCliModule awsCli = new AwsCliModule();

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint.", title = "endpoint")
    @Once
    private String endpoint;

    @Option(name = {"--cluster-id"}, description = "Neptune cluster ID.", title = "clusterId")
    @Once
    private String clusterId;

    @Override
    public void run() {
        try {

            if (StringUtils.isEmpty(endpoint) && StringUtils.isEmpty(clusterId)) {
                throw new IllegalArgumentException("You must supply an endpoint or cluster ID");
            }

            NeptuneClusterMetadata metadata = StringUtils.isNotEmpty(clusterId) ?
                    NeptuneClusterMetadata.createFromClusterId(clusterId, awsCli) :
                    NeptuneClusterMetadata.createFromClusterId(
                            NeptuneClusterMetadata.clusterIdFromEndpoint(endpoint), awsCli);

            printClusterDetails(metadata);

        } catch (Exception e) {
            System.err.println("An error occurred while creating Neptune cluster info:");
            e.printStackTrace();
        }
    }

    public static void printClusterDetails(NeptuneClusterMetadata metadata) {
        System.err.println();
        metadata.printDetails();
    }
}
