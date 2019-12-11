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

package com.amazonaws.services.neptune.propertygraph;

import com.amazonaws.services.neptune.propertygraph.metadata.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class MetadataSamplingSpecification {

    private final boolean sample;
    private final long sampleSize;
    private final boolean requiresMetadata;

    public MetadataSamplingSpecification(boolean sample, long sampleSize, boolean requiresMetadata) {
        this.sample = sample;
        this.sampleSize = sampleSize;
        this.requiresMetadata = requiresMetadata;
    }

    public MetadataCommand createMetadataCommand(Collection<ExportSpecification<?>> exportSpecifications,
                                                 GraphTraversalSource g) {
        if (!requiresMetadata){
            return PropertiesMetadataCollection::new;
        }

        if (sample) {
            return new CreateMetadataFromGraphSample(exportSpecifications, g, sampleSize);
        } else {
            return new CreateMetadataFromGraphScan(exportSpecifications, g);
        }
    }
}
