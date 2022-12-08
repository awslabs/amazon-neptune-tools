    NAME
            neptune-export.sh create-pg-config - Create a property graph schema
            config file.
    
    SYNOPSIS
            neptune-export.sh create-pg-config
                    [ --alb-endpoint <applicationLoadBalancerEndpoint> ]
                    [ {-b | --batch-size} <batchSize> ] [ --clone-cluster ]
                    [ --clone-cluster-correlation-id <cloneCorrelationId> ]
                    [ --clone-cluster-instance-type <cloneClusterInstanceType> ]
                    [ --clone-cluster-replica-count <replicaCount> ]
                    [ {--cluster-id | --cluster | --clusterid} <clusterId> ]
                    [ {-cn | --concurrency} <concurrency> ]
                    {-d | --dir} <directory> [ --disable-ssl ]
                    [ {-e | --endpoint} <endpoint>... ]
                    [ --edge-label-strategy <edgeLabelStrategy> ]
                    [ {-el | --edge-label} <edgeLabels>... ]
                    [ --export-id <exportId> ] [ --format <format> ]
                    [ --gremlin-edge-filter <gremlinEdgeFilter> ]
                    [ --gremlin-filter <gremlinFilter> ]
                    [ --gremlin-node-filter <gremlinNodeFilter> ] [ --janus ]
                    [ --lb-port <loadBalancerPort> ] [ --log-level <log level> ]
                    [ --max-content-length <maxContentLength> ] [ --merge-files ]
                    [ {-nl | --node-label} <nodeLabels>... ]
                    [ --nlb-endpoint <networkLoadBalancerEndpoint> ]
                    [ {-o | --output} <output> ] [ {-p | --port} <port> ]
                    [ --partition-directories <partitionDirectories> ]
                    [ --per-label-directories ] [ --profile <profiles>... ]
                    [ {--region | --stream-region} <region> ]
                    [ {-s | --scope} <scope> ] [ --sample ]
                    [ --sample-size <sampleSize> ] [ --serializer <serializer> ]
                    [ --stream-large-record-strategy <largeStreamRecordHandlingStrategy> ]
                    [ --stream-name <streamName> ] [ {-t | --tag} <tag> ]
                    [ --tokens-only <tokensOnly> ] [ --use-iam-auth ] [ --use-ssl ]
    
    OPTIONS
            --alb-endpoint <applicationLoadBalancerEndpoint>
                Application load balancer endpoint (optional: use only if
                connecting to an IAM DB enabled Neptune cluster through an
                application load balancer (ALB) – see https://github.com/aws-samples/aws-dbs-refarch-graph/tree/master/src/connecting-using-a-load-balancer#connecting-to-amazon-neptune-from-clients-outside-the-neptune-vpc-using-aws-application-load-balancer).
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'load-balancer' from which only
                one option may be specified
    
    
            -b <batchSize>, --batch-size <batchSize>
                Batch size (optional, default 64). Reduce this number if your
                queries trigger CorruptedFrameExceptions.
    
                This option may occur a maximum of 1 times
    
    
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
    
    
            -cn <concurrency>, --concurrency <concurrency>
                Concurrency – the number of parallel queries used to run the export
                (optional, default 4).
    
                This option may occur a maximum of 1 times
    
    
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
    
    
            --edge-label-strategy <edgeLabelStrategy>
                Export edges by their edge labels, or by a combination of their
                start vertex label, edge label, and end vertex label (optional,
                default 'edgeLabelsOnly').
    
                This options value is restricted to the following set of values:
                    edgeLabelsOnly
                    edgeAndVertexLabels
    
                This option may occur a maximum of 1 times
    
    
            -el <edgeLabels>, --edge-label <edgeLabels>
                Labels of edges to be included in config (optional, default all
                labels).
    
            --export-id <exportId>
                Export id
    
                This option may occur a maximum of 1 times
    
    
            --format <format>
                Output format (optional, default 'csv').
    
                This options value is restricted to the following set of values:
                    json
                    csv
                    csvNoHeaders
                    neptuneStreamsJson
                    neptuneStreamsSimpleJson
    
                This option may occur a maximum of 1 times
    
    
            --gremlin-edge-filter <gremlinEdgeFilter>
                Gremlin steps for filtering edges (overrides --gremlin-filter).
    
                This option may occur a maximum of 1 times
    
    
            --gremlin-filter <gremlinFilter>
                Gremlin steps for filtering nodes and edges.
    
                This option may occur a maximum of 1 times
    
    
            --gremlin-node-filter <gremlinNodeFilter>
                Gremlin steps for filtering nodes (overrides --gremlin-filter).
    
                This option may occur a maximum of 1 times
    
    
            --janus
                Use JanusGraph serializer.
    
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
    
    
            --max-content-length <maxContentLength>
                Max content length (optional, default 50000000).
    
                This option may occur a maximum of 1 times
    
    
            --merge-files
                Merge files for each vertex or edge label (currently only supports
                CSV files for export-pg).
    
                This option may occur a maximum of 1 times
    
    
            -nl <nodeLabels>, --node-label <nodeLabels>
                Labels of nodes to be included in config (optional, default all
                labels).
    
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
    
    
            --per-label-directories
                Create a subdirectory for each distinct vertex or edge label.
    
                This option may occur a maximum of 1 times
    
    
            --profile <profiles>
                Name of an export profile.
    
            --region <region>, --stream-region <region>
                AWS Region in which your Amazon Kinesis Data Stream is located.
    
                This option may occur a maximum of 1 times
    
    
            -s <scope>, --scope <scope>
                Scope (optional, default 'all').
    
                This options value is restricted to the following set of values:
                    all
                    nodes
                    edges
    
                This option may occur a maximum of 1 times
    
    
            --sample
                Select only a subset of nodes and edges when generating schema.
    
                This option may occur a maximum of 1 times
    
    
            --sample-size <sampleSize>
                Schema sample size (optional, default 1000).
    
                This option may occur a maximum of 1 times
    
    
            --serializer <serializer>
                Message serializer – (optional, default 'GRAPHBINARY_V1D0').
    
                This options value is restricted to the following set of values:
                    GRAPHSON
                    GRAPHSON_V1D0
                    GRAPHSON_V2D0
                    GRAPHSON_V3D0
                    GRAPHBINARY_V1D0
                    GRYO_V1D0
                    GRYO_V3D0
                    GRYO_LITE_V1D0
    
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
    
    
            --tokens-only <tokensOnly>
                Export tokens (~id, ~label, ~from, ~to) only (optional, default
                'off').
    
                This options value is restricted to the following set of values:
                    off
                    nodes
                    edges
                    both
    
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
            bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output
    
                Create schema config file for all node and edge labels and save it
                to /home/ec2-user/output
    
            bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --sample --sample-size 100
    
                Create schema config file for all node and edge labels, sampling
                100 nodes and edges for each label
    
            bin/neptune-export.sh create-pg-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS
    
                Create config file containing schema for User nodes and FOLLOWS
                edges
    
