    NAME
            neo4j-to-neptune.sh convert-csv - Converts CSV file exported from Neo4j
            via 'apoc.export.csv.all' to Neptune Gremlin load data formatted CSV files,
            and optionally automates the bulk loading of the converted data into Amazon Neptune.

    SYNOPSIS
            neo4j-to-neptune.sh convert-csv [ --bucket-name <bucketName> ]
                    [ --bulk-load-config <bulkLoadConfigFile> ]
                    [ --conversion-config <conversionConfigYAMLFile> ]
                    {-d | --dir} <outputDirectory>
                    [ {-df | --dotenv-file} <envFile> ]
                    [ {-i | --input} <inputFile> ] [ --iam-role-arn <iamRoleArn> ]
                    [ --infer-types ] [ --monitor ]
                    [ {-n | --username} <username> ]
                    [ --neptune-endpoint <neptuneEndpoint> ]
                    [ --node-property-policy <multiValuedNodePropertyPolicy> ]
                    [ --parallelism <parallelism> ]
                    [ {-pw | --password} <password> ]
                    [ --relationship-property-policy <multiValuedRelationshipPropertyPolicy> ]
                    [ --s3-prefix <s3Prefix> ]
                    [ --semi-colon-replacement <semiColonReplacement> ]
                    [ {-u | --uri} <uri> ]

    OPTIONS
            --bucket-name <bucketName>
                S3 bucket name for CSV files to be stored. Overrides bucket-name
                from bulk-load-config file if both are provided.

                This option may occur a maximum of 1 times


            --bulk-load-config <bulkLoadConfigFile>
                Path to YAML file containing configuration for enabling bulk load
                to Neptune. If provided, configuration values are loaded from this
                file first, then overridden by any CLI parameters specified.

                This option may occur a maximum of 1 times


                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and
                writable.


            --conversion-config <conversionConfigFile>
                Path to YAML file containing configuration for label mappings and
                record filtering

                This option may occur a maximum of 1 times


                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and
                writable.


            -d <outputDirectory>, --dir <outputDirectory>
                Root directory for output

                This option may occur a maximum of 1 times


                This options value must be a path to a directory. The provided path
                must be readable and writable.


            -df <envFile>, --dotenv-file <envFile>
                Path to the Dotenv file for Neo4j AuraDB connection.

                This option may occur a maximum of 1 times


                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and
                writable.


                This option is part of the group 'input' from which only one option
                may be specified


            -i <inputFile>, --input <inputFile>
                Path to Neo4j CSV file

                This option may occur a maximum of 1 times


                This options value must be a path to a file. The provided path must
                exist on the file system. The provided path must be readable and
                writable.


                This option is part of the group 'input' from which only one option
                may be specified


            --iam-role-arn <iamRoleArn>
                IAM role ARN for Neptune bulk loading. It will need S3 and Neptune
                access permissions. Overrides iam-role-arn from bulk-load-config
                file if both are provided.
                Refer to the following documentation for the specific
                policies/permissions required:
                https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM-CreateRole.html
                https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM-add-role-cluster.html

                This option may occur a maximum of 1 times


            --infer-types
                Infer data types for CSV column headings

                This option may occur a maximum of 1 times


            --monitor
                Monitor Neptune bulk load progress until completion (default:
                true). Overrides monitor from bulk-load-config file if both are
                provided.

                This option may occur a maximum of 1 times


            -n <username>, --username <username>
                Neo4j Database username

                This option may occur a maximum of 1 times


            --neptune-endpoint <neptuneEndpoint>
                Neptune cluster endpoint. Example:
                my-neptune-cluster.cluster-abc123.<region>.neptune.amazonaws.com.
                Overrides neptune-endpoint from bulk-load-config file if both are
                provided. Either this parameter or --bulk-load-config must be
                provided to enable bulk loading.

                This option may occur a maximum of 1 times


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
                Parallelism level for Neptune bulk loading (default:
                OVERSUBSCRIBE). Overrides parallelism from bulk-load-config file if
                both are provided.

                This options value is restricted to the following set of values:
                    LOW
                    MEDIUM
                    HIGH
                    OVERSUBSCRIBE

                This option may occur a maximum of 1 times


            -pw <password>, --password <password>
                Neo4j Database password

                This option may occur a maximum of 1 times


            --relationship-property-policy <multiValuedRelationshipPropertyPolicy>
                Conversion policy for multi-valued relationship properties
                (default, 'LeaveAsString')

                This options value is restricted to the following set of values:
                    LeaveAsString
                    Halt

                This option may occur a maximum of 1 times


            --s3-prefix <s3Prefix>
                S3 prefix for uploaded file (default: neptune). Overrides s3-prefix
                from bulk-load-config file if both are provided.

                This option may occur a maximum of 1 times


            --semi-colon-replacement <semiColonReplacement>
                Replacement for semi-colon character in multi-value string
                properties (default, ' ')

                This option may occur a maximum of 1 times


                This options value must match the regular expression '^[^;]*$'.
                Replacement string cannot contain a semi-colon.


            -u <uri>, --uri <uri>
                URI of the Neo4j Database to stream data from

                This option may occur a maximum of 1 times


                This option is part of the group 'input' from which only one option
                may be specified
