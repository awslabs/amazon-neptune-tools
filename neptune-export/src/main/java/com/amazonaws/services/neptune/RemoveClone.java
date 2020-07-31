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

import com.amazonaws.services.neptune.cluster.RemoveCloneTask;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;

@Command(name = "remove-clone", description = "Remove a cloned Amazon Neptune database cluster")
public class RemoveClone implements Runnable {

    @Option(name = {"--clone-cluster-id"}, description = "Cluster ID of the cloned Amazon Neptune database cluster")
    @Required
    @Once
    private String cloneClusterId;

    @Override
    public void run() {
        try {
            new RemoveCloneTask(cloneClusterId).execute();
        } catch (Exception e) {
            System.err.println("An error occurred while removing a cloned Amazon Neptune database cluster:");
            e.printStackTrace();
        }
    }
}
