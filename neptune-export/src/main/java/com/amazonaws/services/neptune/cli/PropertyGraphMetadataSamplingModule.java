/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.propertygraph.MetadataSamplingSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.ExportSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.MetadataCommand;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class PropertyGraphMetadataSamplingModule {

    private final RequiresMetadata requiresMetadata;

    @Option(name = {"--sample"}, description = "Select only a subset of nodes and edges when generating property metadata")
    @Once
    private boolean sample = false;

    @Option(name = {"--sample-size"}, description = "Property metadata sample size (optional, default 1000")
    @Once
    private long sampleSize = 1000;

    public PropertyGraphMetadataSamplingModule(RequiresMetadata requiresMetadata) {
        this.requiresMetadata = requiresMetadata;
    }

    public MetadataCommand createMetadataCommand(Collection<ExportSpecification<?>> exportSpecifications,
                                                 GraphTraversalSource g){
        return new MetadataSamplingSpecification(sample, sampleSize, requiresMetadata.requiresMetadata())
                .createMetadataCommand(exportSpecifications, g);
    }
}
