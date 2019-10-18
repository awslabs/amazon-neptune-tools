package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.auth.ConnectionConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class NeptuneExportBaseCommand {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint(s) – supply multiple instance endpoints if you want to load balance requests across a cluster", title = "endpoint")
    @Required
    protected List<String> endpoints = new ArrayList<>();

    @Option(name = {"-p", "--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    protected int port = 8182;

    @Option(name = {"-d", "--dir"}, description = "Root directory for output")
    @Required
    @Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    protected File directory;

    @Option(name = {"-t", "--tag"}, description = "Directory prefix (optional)")
    @Once
    protected String tag = "";

    @Option(name = {"--log-level"}, description = "Log level (optional, default 'error')", title = "log level")
    @Once
    @AllowedValues(allowedValues = {"trace", "debug", "info", "warn", "error"})
    protected String logLevel = "error";

    @Option(name = {"--use-iam-auth"}, description = "Use IAM database authentication to authenticate to Neptune (remember to set SERVICE_REGION environment variable, and, if using a load balancer, set the --host-header option as well)")
    @Once
    protected boolean useIamAuth = false;

    @Option(name = {"--use-ssl"}, description = "Enables connectivity over SSL")
    @Once
    protected boolean useSsl = false;

    @Option(name = {"--nlb-endpoint"}, description = "Network load balancer endpoint (optional: use only if connecting to an IAM DB enabled Neptune cluster through a network load balancer (NLB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-network-load-balancer)")
    @Once
    @MutuallyExclusiveWith(tag = "load-balancer")
    protected String networkLoadBalancerEndpoint;

    @Option(name = {"--alb-endpoint"}, description = "Application load balancer endpoint (optional: use only if connecting to an IAM DB enabled Neptune cluster through an application load balancer (ALB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-application-load-balancer)")
    @Once
    @MutuallyExclusiveWith(tag = "load-balancer")
    protected String applicationLoadBalancerEndpoint;

    @Option(name = {"--lb-port"}, description = "Load balancer port (optional, default 80)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    protected int loadBalancerPort = 80;

    @Option(name = {"--serializer"}, description = "Message serializer – either 'GRAPHBINARY_V1D0' or 'GRYO_V3D0' (optional, default 'GRAPHBINARY_V1D0')")
    @AllowedValues(allowedValues = {"GRAPHBINARY_V1D0", "GRYO_V3D0"})
    @Once
    protected String serializer = "GRAPHBINARY_V1D0";

    @Option(name = {"--max-content-length"}, description = "Max content length (optional, default 65536)")
    @Once
    protected int maxContentLength = 65536;

    public ConnectionConfig connectionConfig() {
        return new ConnectionConfig(
                endpoints,
                port,
                networkLoadBalancerEndpoint,
                applicationLoadBalancerEndpoint,
                loadBalancerPort,
                useIamAuth,
                useSsl,
                Serializers.valueOf(serializer),
                maxContentLength);
    }

    public void applyLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", logLevel);
    }
}
