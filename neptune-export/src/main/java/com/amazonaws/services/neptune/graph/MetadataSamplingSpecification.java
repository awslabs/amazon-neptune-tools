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

package com.amazonaws.services.neptune.graph;

import com.amazonaws.services.neptune.metadata.CreateMetadataFromGraphSample;
import com.amazonaws.services.neptune.metadata.CreateMetadataFromGraphScan;
import com.amazonaws.services.neptune.metadata.MetadataCommand;
import com.amazonaws.services.neptune.metadata.MetadataSpecification;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class MetadataSamplingSpecification {

    private final boolean sample;
    private final long sampleSize;

    public MetadataSamplingSpecification(boolean sample, long sampleSize) {
        this.sample = sample;
        this.sampleSize = sampleSize;
    }

    public MetadataCommand createMetadataCommand(Collection<MetadataSpecification<?>> metadataSpecifications,
                                                 GraphTraversalSource g) {
        if (sample) {
            return new CreateMetadataFromGraphSample(metadataSpecifications, g, sampleSize);
        } else {
            return new CreateMetadataFromGraphScan(metadataSpecifications, g);
        }
    }
}
