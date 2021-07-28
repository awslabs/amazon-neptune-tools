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

package com.amazonaws.services.neptune.propertygraph.schema;

import com.amazonaws.services.neptune.util.Activity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;

public class CreateGraphSchemaFromSample implements CreateGraphSchemaCommand {

    private final Collection<ExportSpecification> exportSpecifications;
    private final GraphTraversalSource g;
    private final long sampleSize;

    public CreateGraphSchemaFromSample(Collection<ExportSpecification> exportSpecifications,
                                       GraphTraversalSource g,
                                       long sampleSize) {
        this.exportSpecifications = exportSpecifications;
        this.sampleSize = sampleSize;
        this.g = g;
    }

    @Override
    public GraphSchema execute() {

        GraphSchema graphSchema = new GraphSchema();
        for (ExportSpecification exportSpecification : exportSpecifications) {

            Timer.timedActivity("creating " + exportSpecification.description() + " schema from sampling graph",
                    (Activity.Runnable) () -> {
                        System.err.println("Creating " + exportSpecification.description() + " schema");
                        exportSpecification.sample(graphSchema, g, sampleSize);
                    });
        }

        return graphSchema;
    }

}
