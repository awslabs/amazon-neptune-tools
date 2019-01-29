    NAME
            neptune-export.sh export-pg-from-config - Export property graph from
            Neptune to CSV or JSON using an existing config file
    
    SYNOPSIS
            neptune-export.sh export-pg-from-config
                    [ --alb-endpoint <albEndpoint> ]
                    {-c | --config-file} <configFile>
                    [ {-cn | --concurrency} <concurrency> ]
                    {-d | --dir} <directory> {-e | --endpoint} <endpoints>...
                    [ {-el | --edge-label} <edgeLabels>... ] [ --format <format> ]
                    [ --lb-port <lbPort> ] [ --log-level <logLevel> ]
                    [ {-nl | --node-label} <nodeLabels>... ]
                    [ --nlb-endpoint <nlbEndpoint> ] [ {-p | --port} <port> ]
                    [ {-r | --range} <range> ] [ {-s | --scope} <scope> ]
                    [ {-t | --tag} <tag> ] [ --use-iam-auth ]
    
    OPTIONS
            --alb-endpoint <albEndpoint>
                Application load balancer endpoint <NEPTUNE_DNS:PORT> (optional
                – use only if connecting to an IAM DB enabled Neptune cluster
                through an application load balancer (ALB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer)
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'load-balancer' from which only
                one option may be specified
    
    
            -c <configFile>, --config-file <configFile>
                Path to JSON config file
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and
                writable.
    
    
            -cn <concurrency>, --concurrency <concurrency>
                Concurrency (optional)
    
                This option may occur a maximum of 1 times
    
    
            -d <directory>, --dir <directory>
                Root directory for output
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a directory. The provided path
                must be readable and writable.
    
    
            -e <endpoints>, --endpoint <endpoints>
                Neptune endpoint(s) – supply multiple instance endpoints if you
                want to load balance requests across a cluster
    
            -el <edgeLabels>, --edge-label <edgeLabels>
                Labels of edges to be exported (optional, default all labels)
    
            --format <format>
                Output format (optional, default 'csv')
    
                This options value is restricted to the following set of values:
                    csv
                    json
    
                This option may occur a maximum of 1 times
    
    
            --lb-port <lbPort>
                Load balancer port (optional, default 80)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1-1023, 1024-49151
    
    
            --log-level <logLevel>
                Log level (optional, default 'error')
    
                This options value is restricted to the following set of values:
                    trace
                    debug
                    info
                    warn
                    error
    
                This option may occur a maximum of 1 times
    
    
            -nl <nodeLabels>, --node-label <nodeLabels>
                Labels of nodes to be exported (optional, default all labels)
    
            --nlb-endpoint <nlbEndpoint>
                Network load balancer endpoint (optional – use only if connecting
                to an IAM DB enabled Neptune cluster through a network load
                balancer (NLB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer)
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'load-balancer' from which only
                one option may be specified
    
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1-1023, 1024-49151
    
    
            -r <range>, --range <range>
                Range (optional)
    
                This option may occur a maximum of 1 times
    
    
            -s <scope>, --scope <scope>
                Scope (optional, default 'all')
    
                This options value is restricted to the following set of values:
                    all
                    nodes
                    edges
    
                This option may occur a maximum of 1 times
    
    
            -t <tag>, --tag <tag>
                Directory prefix (optional)
    
                This option may occur a maximum of 1 times
    
    
            --use-iam-auth
                Use IAM database authentication to authenticate to Neptune
                (remember to set SERVICE_REGION environment variable, and, if using
                a load balancer, set the --host-header option as well)
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-pg-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output
    
                Export data using the metadata config in /home/ec2-user/config.json
    
            bin/neptune-export.sh export-pg-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output --format json
    
                Export data as JSON using the metadata config in
                /home/ec2-user/config.json
    
