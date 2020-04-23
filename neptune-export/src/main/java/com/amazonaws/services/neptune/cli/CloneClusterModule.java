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

import com.amazonaws.services.neptune.auth.ConnectionConfig;
import com.amazonaws.services.neptune.cluster.CloneCluster;
import com.amazonaws.services.neptune.cluster.Cluster;
import com.amazonaws.services.neptune.cluster.DoNotCloneCluster;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class CloneClusterModule {

    @Option(name = {"--clone-cluster"}, description = "Clone Neptune cluster")
    @Once
    private boolean cloneCluster = false;

    public Cluster cloneCluster(ConnectionConfig connectionConfig) throws Exception {
        if (cloneCluster){
            return new CloneCluster().cloneCluster(connectionConfig);
        } else {
            return new DoNotCloneCluster().cloneCluster(connectionConfig);
        }
    }
}
