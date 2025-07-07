    NAME
            neo4j-to-neptune.sh convert-csv - Converts CSV file exported from Neo4j
            via 'apoc.export.csv.all' to Neptune Gremlin load data formatted CSV files,
            and optionally automates the bulk loading of the converted data into Amazon Neptune.

    SYNOPSIS
            neo4j-to-neptune.sh convert-csv [ --bucket-name <bucketName> ]
                    [ --bulk-load ] {-d | --dir} <outputDirectory>
                    [ --conversion-config <conversionConfigYAMLFile> ]
                    {-i | --input} <inputFile> [ --iam-role-arn <iamRoleArn> ]
                    [ --infer-types ] [ --monitor ]
                    [ --neptune-endpoint <neptuneEndpoint> ]
                    [ --node-property-policy <multiValuedNodePropertyPolicy> ]
                    [ --parallelism <parallelism> ]
                    [ --relationship-property-policy <multiValuedRelationshipPropertyPolicy> ]
                    [ --s3-prefix <s3Prefix> ]
                    [ --semi-colon-replacement <semiColonReplacement> ]

    OPTIONS
            --bucket-name <bucketName>
                S3 bucket name for CSV files to be stored

                This option may occur a maximum of 1 times


                This option is required if any of the following options are
                specified: --bulk-load


            --bulk-load
                Enable bulk load to Neptune. If true, the output will be uploaded
                to S3 and loaded into Neptune using the bulk loader (default:
                false)

                This option may occur a maximum of 1 times
                
           
           --conversion-config <conversionConfigYAMLFile>
                Path to conversion configuration YAML file
    
                This option may occur a maximum of 1 times
    
    
                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable.


            -d <outputDirectory>, --dir <outputDirectory>
                Root directory for output

                This option may occur a maximum of 1 times


                This options value must be a path to a directory. The provided path
                must be readable and writable.


            -i <inputFile>, --input <inputFile>
                Path to Neo4j CSV file

                This option may occur a maximum of 1 times


                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and


            --iam-role-arn <iamRoleArn>
                IAM role ARN for Neptune bulk loading

                This option may occur a maximum of 1 times


                This option is required if any of the following options are
                specified: --bulk-load

    
    
            --infer-types
                Infer data types for CSV column headings

                This option may occur a maximum of 1 times


            --monitor
                Monitor Neptune bulk load progress until completion (default: true)

                This option may occur a maximum of 1 times


            --neptune-endpoint <neptuneEndpoint>
                Neptune cluster endpoint. Example:
                my-neptune-cluster.cluster-abc123.<region>.neptune.amazonaws.com

                This option may occur a maximum of 1 times


                This option is required if any of the following options are
                specified: --bulk-load


            --node-property-policy <multiValuedNodePropertyPolicy>
                Conversion policy for multi-valued node properties (default,
                'PutInSetIgnoringDuplicates')

                This options value is restricted to the following set of values:
                    LeaveAsString
                    Halt
                    PutInSetIgnoringDuplicates
                    PutInSetButHaltIfDuplicates

                This option may occur a maximum of 1 times


            --parallelism <parallelism>
                Parallelism level for Neptune bulk loading (default: OVERSUBSCRIBE)

                This options value is restricted to the following set of values:
                    LOW
                    MEDIUM
                    HIGH
                    OVERSUBSCRIBE

                This option may occur a maximum of 1 times


            --relationship-property-policy <multiValuedRelationshipPropertyPolicy>
                Conversion policy for multi-valued relationship properties
                (default, 'LeaveAsString')

                This options value is restricted to the following set of values:
                    LeaveAsString
                    Halt

                This option may occur a maximum of 1 times


            --s3-prefix <s3Prefix>
                S3 prefix for uploaded file (default: neptune)

                This option may occur a maximum of 1 times


            --semi-colon-replacement <semiColonReplacement>
                Replacement for semi-colon character in multi-value string
                properties (default, ' ')

                This option may occur a maximum of 1 times


                This options value must match the regular expression '^[^;]*$'.
                Replacement string cannot contain a semi-colon.
