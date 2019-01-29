package com.amazonaws.services.neptune;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;

import java.io.File;
import java.util.List;

public class NeptuneExportBaseCommand {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint(s) – supply multiple instance endpoints if you want to load balance requests across a cluster")
    @Required
    protected List<String> endpoints;

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

    @Option(name = {"--log-level"}, description = "Log level (optional, default 'error')")
    @Once
    @AllowedValues(allowedValues = {"trace", "debug", "info", "warn", "error"})
    protected String logLevel = "error";

    @Option(name = {"--use-iam-auth"}, description = "Use IAM database authentication to authenticate to Neptune (remember to set SERVICE_REGION environment variable, and, if using a load balancer, set the --host-header option as well)")
    @Once
    protected boolean useIamAuth = false;

    @Option(name = {"--host-header"}, description = "Host header (<NEPTUNE_DNS:PORT> if connecting to an IAM DB enabled Neptune cluster through a load balancer – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer)")
    @Once
    protected String hostHeader = "";

    public void setLoggingLevel(){
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", logLevel);
    }
}
