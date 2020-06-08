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
import com.amazonaws.services.neptune.cluster.NeptuneClusterMetadata;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class CommonConnectionModule {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint(s) – supply multiple instance endpoints if you want to load balance requests across a cluster", title = "endpoint")
    private Collection<String> endpoints = new HashSet<>();

    @Option(name = {"--cluster-id"}, description = "ID of an Amazon Neptune cluster. If you specify a cluster ID, neptune-export will use all of the instance endpoints in the cluster in addition to any endpoints you have specified using the -e and --endpoint options.")
    @Once
    private String clusterId;

    @Option(name = {"-p", "--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    private int port = 8182;

    @Option(name = {"--use-iam-auth"}, description = "Use IAM database authentication to authenticate to Neptune (remember to set SERVICE_REGION environment variable)")
    @Once
    private boolean useIamAuth = false;

    @Option(name = {"--use-ssl"}, description = "Enables connectivity over SSL")
    @Once
    private boolean useSsl = false;

    @Option(name = {"--nlb-endpoint"}, description = "Network load balancer endpoint (optional: use only if connecting to an IAM DB enabled Neptune cluster through a network load balancer (NLB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-network-load-balancer)")
    @Once
    @MutuallyExclusiveWith(tag = "load-balancer")
    private String networkLoadBalancerEndpoint;

    @Option(name = {"--alb-endpoint"}, description = "Application load balancer endpoint (optional: use only if connecting to an IAM DB enabled Neptune cluster through an application load balancer (ALB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-application-load-balancer)")
    @Once
    @MutuallyExclusiveWith(tag = "load-balancer")
    private String applicationLoadBalancerEndpoint;

    @Option(name = {"--lb-port"}, description = "Load balancer port (optional, default 80)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    private int loadBalancerPort = 80;

    public ConnectionConfig config() {

        if (StringUtils.isNotEmpty(clusterId)){
            NeptuneClusterMetadata clusterMetadata = NeptuneClusterMetadata.createFromClusterId(clusterId);
            endpoints.addAll(clusterMetadata.endpoints());
        }

        if (endpoints.isEmpty()){
            throw new IllegalStateException("You must supply a cluster ID or one or more endpoints");
        }

        return new ConnectionConfig(
                endpoints,
                port,
                networkLoadBalancerEndpoint,
                applicationLoadBalancerEndpoint,
                loadBalancerPort,
                useIamAuth,
                useSsl);
    }
}
