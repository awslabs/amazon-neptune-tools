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

public class CreateMetadataFromGraphScan implements MetadataCommand {
    private final Collection<ExportSpecification<?>> exportSpecifications;
    private final GraphTraversalSource g;

    public CreateMetadataFromGraphScan(Collection<ExportSpecification<?>> exportSpecifications,
                                       GraphTraversalSource g) {
        this.exportSpecifications = exportSpecifications;
        this.g = g;
    }

    @Override
    public PropertiesMetadataCollection execute() throws Exception {
        PropertiesMetadataCollection metadataCollection = new PropertiesMetadataCollection();
        for (ExportSpecification<?> exportSpecification : exportSpecifications) {
            try (Timer timer = new Timer("creating " + exportSpecification.description() + " metadata from graph scan")) {
                System.err.println("Creating " + exportSpecification.description() + " metadata");
                exportSpecification.scan(metadataCollection, g);
            }
        }
        return metadataCollection;
    }
}
