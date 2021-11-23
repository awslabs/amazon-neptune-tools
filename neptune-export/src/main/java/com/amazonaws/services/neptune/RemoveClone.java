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
import com.amazonaws.services.neptune.cli.CloneClusterModule;
import com.amazonaws.services.neptune.cluster.GetClusterIdFromCorrelationId;
import com.amazonaws.services.neptune.cluster.RemoveCloneTask;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;

@Command(name = "remove-clone", description = "Remove a cloned Amazon Neptune database cluster.")
public class RemoveClone implements Runnable {

    @Inject
    private AwsCliModule awsCli = new AwsCliModule();

    @Option(name = {"--clone-cluster-id"}, description = "Cluster ID of the cloned Amazon Neptune database cluster.")
    @RequireOnlyOne(tag = "cloneClusterIdOrCorrelationId")
    @Once
    private String cloneClusterId;

    @Option(name = {"--clone-cluster-correlation-id"}, description = "Value of the correlation-id tag on an Amazon Neptune cloned cluster that you want to remove.")
    @RequireOnlyOne(tag = "cloneClusterIdOrCorrelationId")
    @Once
    private String correlationId;

    @Override
    public void run() {

        if (StringUtils.isEmpty(cloneClusterId) && StringUtils.isNotEmpty(correlationId)){
            cloneClusterId = new GetClusterIdFromCorrelationId(correlationId, awsCli).execute();
            if (StringUtils.isEmpty(cloneClusterId)){
                System.err.println(String.format("Unable to get a cloned Amazon Neptune database cluster ID for correlation ID %s", correlationId));
                System.exit(0);
            }
        }

        try {
            new RemoveCloneTask(cloneClusterId, awsCli).execute();
        } catch (Exception e) {
            System.err.println("An error occurred while removing a cloned Amazon Neptune database cluster:");
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
