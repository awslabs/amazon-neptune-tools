    NAME
            neptune-export.sh remove-clone - Remove a cloned Amazon Neptune
            database cluster.
    
    SYNOPSIS
            neptune-export.sh remove-clone
                    [ --clone-cluster-correlation-id <correlationId> ]
                    [ --clone-cluster-id <cloneClusterId> ]
    
    OPTIONS
            --clone-cluster-correlation-id <correlationId>
                Value of the correlation-id tag on an Amazon Neptune cloned cluster
                that you want to remove.
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'cloneClusterIdOrCorrelationId'
                from which only one option may be specified
    
    
            --clone-cluster-id <cloneClusterId>
                Cluster ID of the cloned Amazon Neptune database cluster.
    
                This option may occur a maximum of 1 times
    
    
                This option is part of the group 'cloneClusterIdOrCorrelationId'
                from which only one option may be specified
    
    
