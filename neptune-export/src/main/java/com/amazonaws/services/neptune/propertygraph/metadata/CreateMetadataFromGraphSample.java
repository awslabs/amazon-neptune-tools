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

package com.amazonaws.services.neptune.propertygraph.metadata;

import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class CreateMetadataFromGraphSample implements MetadataCommand {

    private final Collection<ExportSpecification<?>> exportSpecifications;
    private final GraphTraversalSource g;
    private final long sampleSize;

    public CreateMetadataFromGraphSample(Collection<ExportSpecification<?>> exportSpecifications,
                                         GraphTraversalSource g,
                                         long sampleSize) {
        this.exportSpecifications = exportSpecifications;
        this.sampleSize = sampleSize;
        this.g = g;
    }

    @Override
    public PropertiesMetadataCollection execute() throws Exception {

        PropertiesMetadataCollection metadataCollection = new PropertiesMetadataCollection();
        for (ExportSpecification<?> exportSpecification : exportSpecifications) {
            try (Timer timer = new Timer("creating " + exportSpecification.description() + " metadata from sampling graph")) {
                System.err.println("Creating " + exportSpecification.description() + " metadata");
                exportSpecification.sample(metadataCollection, g, sampleSize);
            }
        }
        return metadataCollection;
    }

}
