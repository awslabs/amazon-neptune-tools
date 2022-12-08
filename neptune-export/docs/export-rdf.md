    NAME
            neptune-export.sh export-rdf - Export RDF graph from Neptune.
    
    SYNOPSIS
            neptune-export.sh export-rdf
                    [ --alb-endpoint <applicationLoadBalancerEndpoint> ]
                    [ --clone-cluster ]
                    [ --clone-cluster-correlation-id <cloneCorrelationId> ]
                    [ --clone-cluster-instance-type <cloneClusterInstanceType> ]
                    [ --clone-cluster-replica-count <replicaCount> ]
                    [ {--cluster-id | --cluster | --clusterid} <clusterId> ]
                    {-d | --dir} <directory> [ --disable-ssl ]
                    [ {-e | --endpoint} <endpoint>... ] [ --export-id <exportId> ]
                    [ --format <format> ] [ --include-last-event-id ]
                    [ --lb-port <loadBalancerPort> ] [ --log-level <log level> ]
                    [ --nlb-endpoint <networkLoadBalancerEndpoint> ]
                    [ {-o | --output} <output> ] [ {-p | --port} <port> ]
                    [ --partition-directories <partitionDirectories> ]
                    [ --profile <profiles>... ] [ --rdf-export-scope <scope> ]
                    [ {--region | --stream-region} <region> ] [ --sparql <query> ]
                    [ --stream-large-record-strategy <largeStreamRecordHandlingStrategy> ]
                    [ --stream-name <streamName> ] [ {-t | --tag} <tag> ]
                    [ --use-iam-auth ] [ --use-ssl ]
    
    OPTIONS
            --alb-endpoint <applicationLoadBalancerEndpoint>
                Application load balancer endpoint (optional: use only if
                connecting to an IAM DB enabled Neptune cluster through an
                application load balancer (ALB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-application-load-balancer).
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'load-balancer' from which only
                one option may be specified
    
    
            --clone-cluster
                Clone an Amazon Neptune cluster.
    
                This option may occur a maximum of 1 times
    
    
            --clone-cluster-correlation-id <cloneCorrelationId>
                Correlation ID to be added to a correlation-id tag on the cloned
                cluster.
    
                This option may occur a maximum of 1 times
    
    
            --clone-cluster-instance-type <cloneClusterInstanceType>
                Instance type for cloned cluster (by default neptune-export will
                use the same instance type as the source cluster).
    
                This options value is restricted to the following set of values:
                    db.r4.large
                    db.r4.xlarge
                    db.r4.2xlarge
                    db.r4.4xlarge
                    db.r4.8xlarge
                    db.r5.large
                    db.r5.xlarge
                    db.r5.2xlarge
                    db.r5.4xlarge
                    db.r5.8xlarge
                    db.r5.12xlarge
                    db.t3.medium
                    r4.large
                    r4.xlarge
                    r4.2xlarge
                    r4.4xlarge
                    r4.8xlarge
                    r5.large
                    r5.xlarge
                    r5.2xlarge
                    r5.4xlarge
                    r5.8xlarge
                    r5.12xlarge
                    t3.medium
    
                This option may occur a maximum of 1 times
    
    
            --clone-cluster-replica-count <replicaCount>
                Number of read replicas to add to the cloned cluster (default, 0).
    
                This option may occur a maximum of 1 times
    
    
                This options value must fall in the following range: 0 <= value <= 15
    
    
            --cluster-id <clusterId>, --cluster <clusterId>, --clusterid
            <clusterId>
                ID of an Amazon Neptune cluster. If you specify a cluster ID,
                neptune-export will use all of the instance endpoints in the
                cluster in addition to any endpoints you have specified using the
                endpoint options.
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'endpoint or clusterId' from which
                at least one option must be specified
    
    
            -d <directory>, --dir <directory>
                Root directory for output.
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a directory. The provided path
                must be readable and writable.
    
    
            --disable-ssl
                Disables connectivity over SSL.
    
                This option may occur a maximum of 1 times
    
    
            -e <endpoint>, --endpoint <endpoint>
                Neptune endpoint(s) – supply multiple instance endpoints if you
                want to load balance requests across a cluster.
    
                This option is part of the group 'endpoint or clusterId' from which
                at least one option must be specified
    
    
            --export-id <exportId>
                Export ID
    
                This option may occur a maximum of 1 times
    
    
            --format <format>
                Output format (optional, default 'turtle').
    
                This options value is restricted to the following set of values:
                    turtle
                    nquads
                    ntriples
                    neptuneStreamsJson
                    neptuneStreamsSimpleJson
    
                This option may occur a maximum of 1 times
    
    
            --include-last-event-id
                Get the last event ID from the Amazon Neptune stream, if enabled,
                and save it to a JSON file (optional, default 'false').
    
                This option may occur a maximum of 1 times
    
    
            --lb-port <loadBalancerPort>
                Load balancer port (optional, default 80).
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1-1023, 1024-49151
    
    
            --log-level <log level>
                Log level (optional, default 'error').
    
                This options value is restricted to the following set of values:
                    trace
                    debug
                    info
                    warn
                    error
    
                This option may occur a maximum of 1 times
    
    
            --nlb-endpoint <networkLoadBalancerEndpoint>
                Network load balancer endpoint (optional: use only if connecting to
                an IAM DB enabled Neptune cluster through a network load balancer
                (NLB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-network-load-balancer).
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'load-balancer' from which only
                one option may be specified
    
    
            -o <output>, --output <output>
                Output target (optional, default 'file').
    
                This options value is restricted to the following set of values:
                    files
                    stdout
                    devnull
                    stream
    
                This option may occur a maximum of 1 times
    
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182).
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1-1023, 1024-49151
    
    
            --partition-directories <partitionDirectories>
                Partition directory path (e.g. 'year=2021/month=07/day=21').
    
                This option may occur a maximum of 1 times
    
    
            --profile <profiles>
                Name of an export profile.
    
            --rdf-export-scope <scope>
                Export scope (optional, default 'graph').
    
                This options value is restricted to the following set of values:
                    graph
                    edges
                    query
    
                This option may occur a maximum of 1 times
    
    
            --region <region>, --stream-region <region>
                AWS Region in which your Amazon Kinesis Data Stream is located.
    
                This option may occur a maximum of 1 times
    
    
            --sparql <query>
                SPARQL query.
    
                This option may occur a maximum of 1 times
    
    
            --stream-large-record-strategy <largeStreamRecordHandlingStrategy>
                Strategy for dealing with records to be sent to Amazon Kinesis that
                are larger than 1 MB.
    
                This options value is restricted to the following set of values:
                    dropAll
                    splitAndDrop
                    splitAndShred
    
                This option may occur a maximum of 1 times
    
    
            --stream-name <streamName>
                Name of an Amazon Kinesis Data Stream.
    
                This option may occur a maximum of 1 times
    
    
            -t <tag>, --tag <tag>
                Directory prefix (optional).
    
                This option may occur a maximum of 1 times
    
    
            --use-iam-auth
                Use IAM database authentication to authenticate to Neptune
                (remember to set the SERVICE_REGION environment variable).
    
                This option may occur a maximum of 1 times
    
    
            --use-ssl
                Enables connectivity over SSL. This option is
                deprecated: neptune-export will always connect via SSL unless you
                use --disable-ssl to explicitly disable connectivity over SSL.
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-rdf -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output
    
                Export all data to the /home/ec2-user/output directory
    
