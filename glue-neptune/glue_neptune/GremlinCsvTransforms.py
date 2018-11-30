# Copyright 2018 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

import sys, os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.transforms import RenameField
from awsglue.transforms import SelectFields
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import format_string

class GremlinCsvTransforms:
    
    @classmethod
    def create_prefixed_columns(cls, datasource, mappings):
        """Creates columns in a DynamicFrame whose values are based on prefixed values from another column in the DynamicFrame.
        
        Example:
        >>> df = GremlinCsvTransforms.create_prefixed_columns(df, [('~id', 'productId', 'p'),('~to', 'supplierId', 's')])
        """
        dataframe = datasource.toDF()
        for (column_name, source_column, prefix) in mappings:
            dataframe = dataframe.withColumn(column_name, format_string(prefix + "-%s", dataframe[source_column]))
        return DynamicFrame.fromDF(dataframe, datasource.glue_ctx, 'create_vertex_id_columns')
    
    @classmethod
    def create_edge_id_column(cls, datasource, from_column, to_column):
        """Creates an '~id' column in a DynamicFrame whose values are based on the specified from and to columns.
        
        Example:
        >>> df = GremlinCsvTransforms.create_edge_id_column(df, 'supplierId', 'productId')
        """
        dataframe = datasource.toDF()
        dataframe = dataframe.withColumn('~id', format_string("%s-%s", dataframe[from_column], dataframe[to_column]))
        return DynamicFrame.fromDF(dataframe,  datasource.glue_ctx, 'create_edge_id_column')
    
    @classmethod    
    def addLabel(cls, datasource, label):
        """Adds a '~label' column to a DynamicFrame whose values comprise the supplier label.
        
        Example:
        >>> df = GremlinCsvTransforms.addLabel(df, 'Product')
        """
        dataframe = datasource.toDF()
        dataframe = dataframe.withColumn("~label", lit(label))
        return DynamicFrame.fromDF(dataframe, datasource.glue_ctx, label)