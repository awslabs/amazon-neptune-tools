    NAME
            neptune-export.sh export-rdf - Export RDF graph from Neptune to Turtle
    
    SYNOPSIS
            neptune-export.sh export-rdf [ --alb-host-header <albHostHeader> ]
                    {-d | --dir} <directory> {-e | --endpoint} <endpoints>...
                    [ --log-level <logLevel> ]
                    [ --nlb-host-header <nlbHostHeader> ] [ {-p | --port} <port> ]
                    [ {-t | --tag} <tag> ] [ --use-iam-auth ]
    
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
    
            --log-level <logLevel>
                Log level (optional, default 'error')
    
                This options value is restricted to the following set of values:
                    trace
                    debug
                    info
                    warn
                    error
    
                This option may occur a maximum of 1 times
    
    
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
    
    
            -t <tag>, --tag <tag>
                Directory prefix (optional)
    
                This option may occur a maximum of 1 times
    
    
            --use-iam-auth
                Use IAM database authentication to authenticate to Neptune
                (remember to set SERVICE_REGION environment variable, and, if using
                a load balancer, set the --host-header option as well)
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-rdf -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output
    
                Export all data to the /home/ec2-user/output directory
    
