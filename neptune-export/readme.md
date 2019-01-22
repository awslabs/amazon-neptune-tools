# Neptune Export

Exports Amazon Neptune property graph data to CSV or JSON, or RDF graph data to Turtle.

### Property Graph

  - [Exporting to the bulk loader CSV format](#exporting-to-the-bulk-loader-csv-format)
  - [Exporting the results of user-supplied queries](#exporting-the-results-of-user-supplied-queries)
  
### RDF Graph

  - [Exporting an RDF graph](#exporting-an-rdf-graph)

## Exporting to the bulk loader CSV format

When exporting to the [CSV format](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html) used by the [Amazon Neptune bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html), _neptune-export_ generates CSV files based on metadata derived from scanning your graph. This metadata is persisted in a JSON file. There are three ways in which you can use the tool to generate bulk load files:

 - `export` – This command makes two passes over your data: the first to generate the metadata, the second to create the data files. By scanning all nodes and edges in the first pass, the tool captures the superset of properties for each label, identifies the broadest datatype for each property, and identifies any properties for which at least one vertex or edge has multiple values. If exporting to CSV, these latter properties are exported to CSV as array types. If exporting to JSON, these property values are exported as array nodes.
 - `create-config` – This command makes a single pass over your data to generate the metadata config file.
 - `export-from-config` – This command makes a single pass over your data to create the CSV or JSON files. It uses a preexisting metadata config file.
 
### Generating metadata

`export` and `create-config` both generate metadata JSON files describing the properties associated with each node and edge label. By default, these commands will scan the entire database. For large datasets, this can take a long time. 

Both commands also allow you to sample a range of nodes and edges in order to create this metadata. If you are confident that sampling your data will yield the same metadata as scanning the entire dataset, specify the `--sample` option with these commands. If, however, you have reason to believe the same property on different nodes or edges could yield different datatypes, or different cardinalities, or that nodes or edges with the same labels could contain different sets of properties, you should consider retaining the default behaviour of a full scan.

### Label filters

All three commands allow you to supply vertex and edge label filters. 

 - If you supply label filters to the `export-pg` command, the metadata file and the exported data files will contain data only for the labels specified in the filters.
 - If you supply label filters to the `create-pg-config` command, the metadata file will contain data only for the labels specified in the filters.
 - If you supply label filters to the `export-pg-from-config` command, the exported data files will contain data for the intersection of labels in the config file and the labels specified in the command filters.
 
### Parallel export

The `export` and `export-from-config` commands support parallel export. You can supply a concurrency level, which determines the number of client threads used to perform the parallel export, and, optionally, a range or batch size, which determines how many nodes or edges will be queried by each thread at a time. If you specify a concurrency level, but don't supply a range, the tool will calculate a range such that each thread queries _(1/concurrency level) * number of nodes/edges_ nodes or edges.

If using parallel export, we recommend setting the concurrency level to the number of vCPUs on your Neptune instance.

You can load balance requests across multiple instances in your cluster (or even multiple clusters) by supplying multiple `--endpoint` options.

### Long-running queries

_neptune-export_ uses long-running queries to generate the metadata and the data files. You may need to increase the `neptune_query_timeout` [DB parameter](https://docs.aws.amazon.com/neptune/latest/userguide/parameters.html) in order to run the tool against large datasets.

For large datasets, we recommend running this tool against a standalone database instance that has been restored from a snapshot of your database.

## Build

`mvn clean install`

## Usage

### export-pg

    NAME
            neptune-export.sh export-pg - Export property graph from Neptune to CSV or JSON
    
    SYNOPSIS
            neptune-export.sh export-pg [ {-cn | --concurrency} <concurrency> ]
                    {-d | --dir} <directory> {-e | --endpoint} <endpoints>...
                    [ {-el | --edge-label} <edgeLabels>... ] [ --format <format> ]
                    [ {-nl | --node-label} <nodeLabels>... ]
                    [ {-p | --port} <port> ] [ {-r | --range} <range> ]
                    [ {-s | --scope} <scope> ] [ --sample ]
                    [ --sample-size <sampleSize> ] [ {-t | --tag} <tag> ]
                    [ --use-iam-auth ]
    
    OPTIONS
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
    
    
            -nl <nodeLabels>, --node-label <nodeLabels>
                Labels of nodes to be exported (optional, default all labels)
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1024-49151
    
    
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
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output
    
                Export all data to the /home/ec2-user/output directory
    
            bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output --format json
    
                Export all data to the /home/ec2-user/output directory as JSON
    
            bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -s nodes
    
                Export only nodes to the /home/ec2-user/output directory
    
            bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -nl User -el FOLLOWS
    
                Export only User nodes and FOLLOWS relationships
    
            bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -cn 2
    
                Parallel export using 2 threads
    
            bin/neptune-export.sh export-pg -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -cn 2 -r 1000
    
                Parallel export using 2 threads, with each thread processing
                batches of 1000 nodes or edges

### create-pg-config

    NAME
            neptune-export.sh create-pg-config - Create a property graph export metadata config file
    
    SYNOPSIS
            neptune-export.sh create-pg-config {-d | --dir} <directory>
                    {-e | --endpoint} <endpoints>...
                    [ {-el | --edge-label} <edgeLabels>... ]
                    [ {-nl | --node-label} <nodeLabels>... ]
                    [ {-p | --port} <port> ] [ {-s | --scope} <scope> ]
                    [ --sample ] [ --sample-size <sampleSize> ]
                    [ {-t | --tag} <tag> ] [ --use-iam-auth ]
    
    OPTIONS
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
    
            -nl <nodeLabels>, --node-label <nodeLabels>
                Labels of nodes to be included in config (optional, default all
                labels)
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1024-49151
    
    
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

### export-pg-from-config

    NAME
            neptune-export.sh export-pg-from-config - Export from Neptune to CSV or
            JSON using an existing config file
    
    SYNOPSIS
            neptune-export.sh export-pg-from-config {-c | --config-file} <configFile>
                    [ {-cn | --concurrency} <concurrency> ]
                    {-d | --dir} <directory> {-e | --endpoint} <endpoints>...
                    [ {-el | --edge-label} <edgeLabels>... ] [ --format <format> ]
                    [ {-nl | --node-label} <nodeLabels>... ]
                    [ {-p | --port} <port> ] [ {-r | --range} <range> ]
                    [ {-s | --scope} <scope> ] [ {-t | --tag} <tag> ]
                    [ --use-iam-auth ]
    
    OPTIONS
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
    
    
            -nl <nodeLabels>, --node-label <nodeLabels>
                Labels of nodes to be exported (optional, default all labels)
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1024-49151
    
    
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
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-pg-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output
    
                Export data using the metadata config in /home/ec2-user/config.json
    
            bin/neptune-export.sh export-pg-from-config -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -c /home/ec2-user/config.json -d /home/ec2-user/output --format json
    
                Export data as JSON using the metadata config in
                /home/ec2-user/config.json

## Exporting the results of user-supplied queries

_neptune-export_'s `export-from-queries` command allows you to supply groups of Gremlin queries and export the results to CSV or JSON.

Every user-supplied query should return a resultset whose every result comprises a Map. Typically, these are queries that return a `valueMap()` or a projection created using `project().by().by()...`.

Queries are grouped into _named groups_. All the queries in a named group should return the same columns. Named groups allow you to 'shard' large queries and execute them in parallel (using the `--concurrency` option). The resulting CSV or JSON files will be written to a directory named after the group.

You can supply multiple named groups using multiple `--queries` options. Each group comprises a name, an equals sign, and  then a semi-colon-delimited list of Gremlin queries. Surround the list of queries in double quotes. For example:

`-q person="g.V().hasLabel('Person').range(0,100000).valueMap();g.V().hasLabel('Person').range(100000,-1).valueMap()"`

Alternatively, you can supply a JSON file of queries.

### Parallel execution of queries

If using parallel export, we recommend setting the concurrency level to the number of vCPUs on your Neptune instance. When _neptune-export_ executes named groups of queries in parallel, it simply flattens all the queries into a queue, and spins up a pool of worker threads according to the concurrency level you have specified using `--concurrency`. Worker threads continue to take queries from the queue until the queue is exhausted.

### Batching

Queries whose results contain very large rows can sometimes trigger a `CorruptedFrameException`. If this happens, adjust the batch size (`--batch-size`) to reduce the number of results returned to the client in a batch (the default is 64).

### export-pg-from-queries

    NAME
            neptune-export.sh export-pg-from-queries - Export property graph to CSV or JSON from Gremlin queries
            Gremlin queries
    
    SYNOPSIS
            neptune-export.sh export-pg-from-queries
                    [ {-b | --batch-size} <batchSize> ]
                    [ {-cn | --concurrency} <concurrency> ]
                    {-d | --dir} <directory> {-e | --endpoint} <endpoints>...
                    [ {-f | --queries-file} <queriesFile> ] [ --format <format> ]
                    [ {-p | --port} <port> ] [ {-q | --queries} <queries>... ]
                    [ {-t | --tag} <tag> ] [ --use-iam-auth ]
    
    OPTIONS
            -b <batchSize>, --batch-size <batchSize>
                Batch size (optional, default 64). Reduce this number if your
                queries trigger CorruptedFrameExceptions.
    
                This option may occur a maximum of 1 times
    
    
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
    
            -f <queriesFile>, --queries-file <queriesFile>
                Path to JSON queries file
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and
                writable.
    
    
            --format <format>
                Output format (optional, default 'csv')
    
                This options value is restricted to the following set of values:
                    csv
                    json
    
                This option may occur a maximum of 1 times
    
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1024-49151
    
    
            -q <queries>, --queries <queries>
                Gremlin queries (format: name="semi-colon-separated list of
                queries")
    
            -t <tag>, --tag <tag>
                Directory prefix (optional)
    
                This option may occur a maximum of 1 times
    
    
            --use-iam-auth
                Use IAM database authentication to authenticate to Neptune
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-pg-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person="g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')" -q post="g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())" --concurrency 6
    
                Parallel export of Person data in 2 shards, sharding on the 'birthday' property, 
                and Post data in 4 shards, sharding on range, using 6 threads
    
            bin/neptune-export.sh export-pg-from-queries -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output -q person="g.V().hasLabel('Person').has('birthday', lt('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday');g.V().hasLabel('Person').has('birthday', gte('1985-01-01')).project('id', 'first_name', 'last_name', 'birthday').by(id).by('firstName').by('lastName').by('birthday')" -q post="g.V().hasLabel('Post').has('imageFile').range(0, 250000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(250000, 500000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(500000, 750000).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id());g.V().hasLabel('Post').has('imageFile').range(750000, -1).project('id', 'image_file', 'creation_date', 'creator_id').by(id).by('imageFile').by('creationDate').by(in('CREATED').id())" --concurrency 6 --format json
    
                Parallel export of Person data and Post data as JSON

## Exporting an RDF graph
              
### export-rdf

    NAME
            neptune-export.sh export-rdf - Export RDF graph from Neptune to Turtle
    
    SYNOPSIS
            neptune-export.sh export-rdf {-d | --dir} <directory>
                    {-e | --endpoint} <endpoints>... [ {-p | --port} <port> ]
                    [ {-t | --tag} <tag> ]
    
    OPTIONS
            -d <directory>, --dir <directory>
                Root directory for output
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a directory. The provided path
                must be readable and writable.
    
    
            -e <endpoints>, --endpoint <endpoints>
                Neptune endpoint(s) – supply multiple instance endpoints if you
                want to load balance requests across a cluster
    
            -p <port>, --port <port>
                Neptune port (optional, default 8182)
    
                This option may occur a maximum of 1 times
    
    
                This options value represents a port and must fall in one of the
                following port ranges: 1024-49151
    
    
            -t <tag>, --tag <tag>
                Directory prefix (optional)
    
                This option may occur a maximum of 1 times
    
    
    EXAMPLES
            bin/neptune-export.sh export-rdf -e neptunedbcluster-xxxxxxxxxxxx.cluster-yyyyyyyyyyyy.us-east-1.neptune.amazonaws.com -d /home/ec2-user/output
    
                Export all data to the /home/ec2-user/output directory