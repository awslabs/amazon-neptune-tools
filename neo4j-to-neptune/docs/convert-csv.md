    NAME
            neo4j-to-neptune.sh convert-csv - Converts CSV file exported from Neo4j
            via 'apoc.export.csv.all' to Neptune Gremlin import CSV files
    
    SYNOPSIS
            neo4j-to-neptune.sh convert-csv {-d | --dir} <outputDirectory>
                    {-i | --input} <inputFile> [ --infer-types ]
                    [ --node-property-policy <multiValuedNodePropertyPolicy> ]
                    [ --relationship-property-policy <multiValuedRelationshipPropertyPolicy> ]
                    [ --semi-colon-replacement <semiColonReplacement> ]
    
    OPTIONS
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
                writable.
    
    
            --infer-types
                Infer data types for CSV column headings
    
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
    
    
            --relationship-property-policy <multiValuedRelationshipPropertyPolicy>
                Conversion policy for multi-valued relationship properties
                (default, 'LeaveAsString')
    
                This options value is restricted to the following set of values:
                    LeaveAsString
                    Halt
    
                This option may occur a maximum of 1 times
    
    
            --semi-colon-replacement <semiColonReplacement>
                Replacement for semi-colon character in multi-value string
                properties (default, ' ')
    
                This option may occur a maximum of 1 times
    
    
                This options value must match the regular expression '^[^;]*$'.
                Replacement string cannot contain a semi-colon.
    
    
