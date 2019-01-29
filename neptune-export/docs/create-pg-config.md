    NAME
            neptune-export.sh create-pg-config - Create a property graph export
            metadata config file
    
    SYNOPSIS
            neptune-export.sh create-pg-config
                    [ --alb-host-header <albHostHeader> ] {-d | --dir} <directory>
                    {-e | --endpoint} <endpoints>...
                    [ {-el | --edge-label} <edgeLabels>... ]
                    [ --log-level <logLevel> ]
                    [ {-nl | --node-label} <nodeLabels>... ]
                    [ --nlb-host-header <nlbHostHeader> ] [ {-p | --port} <port> ]
                    [ {-s | --scope} <scope> ] [ --sample ]
                    [ --sample-size <sampleSize> ] [ {-t | --tag} <tag> ]
                    [ --use-iam-auth ]
    
    OPTIONS
            --alb-host-header <albHostHeader>
                Host header of the form <NEPTUNE_DNS:PORT> (optional – use only if
                connecting to an IAM DB enabled Neptune cluster through an
                application load balancer (ALB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer)
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'host-header' from which only one
                option may be specified
    
    
            -d <directory>, --dir <directory>
                Root directory for output
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a directory. The provided path
                must be readable and writable.
    
    
            -e <endpoints>, --endpoint <endpoints>
                Neptune endpoint(s) – supply multiple instance endpoints if you
                want to load balance requests across a cluster
    
            -el <edgeLabels>, --edge-label <edgeLabels>
                Labels of edges to be included in config (optional, default all
                labels)
    
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
                Labels of nodes to be included in config (optional, default all
                labels)
    
            --nlb-host-header <nlbHostHeader>
                Host header of the form <NEPTUNE_DNS:PORT> (optional – use only if
                connecting to an IAM DB enabled Neptune cluster through a network
                load balancer (NLB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer)
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'host-header' from which only one
                option may be specified
    
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1-1023, 1024-49151
    
    
            -s <scope>, --scope <scope>
                Scope (optional, default 'all')
    
                This options value is restricted to the following set of values:
                    all
                    nodes
                    edges
    
                This option may occur a maximum of 1 times
    
    
            --sample
                Select only a subset of nodes and edges when generating property
                metadata
    
                This option may occur a maximum of 1 times
    
    
            --sample-size <sampleSize>
                Property metadata sample size (optional, default 1000
    
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
            bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output
    
                Create metadata config file for all node and edge labels and save
                it to /home/ec2-user/output
    
            bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --sample --sample-size 100
    
                Create metadata config file for all node and edge labels, sampling
                100 nodes and edges for each label
    
            bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS
    
                Create config file containing metadata for User nodes and FOLLOWS
                edges
    
