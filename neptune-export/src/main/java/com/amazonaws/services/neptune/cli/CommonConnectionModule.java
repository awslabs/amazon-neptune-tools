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
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;

import java.util.ArrayList;
import java.util.List;

public class CommonConnectionModule {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint(s) – supply multiple instance endpoints if you want to load balance requests across a cluster", title = "endpoint")
    @Required
    private List<String> endpoints = new ArrayList<>();

    @Option(name = {"-p", "--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    private int port = 8182;

    @Option(name = {"--use-iam-auth"}, description = "Use IAM database authentication to authenticate to Neptune (remember to set SERVICE_REGION environment variable, and, if using a load balancer, set the --host-header option as well)")
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
