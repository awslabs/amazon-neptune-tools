/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.*;

public class MasterLabelSchemas {

    public static MasterLabelSchemas fromCollection(Collection<FileSpecificLabelSchemas> fileSpecificLabelSchemasCollection) {

        Set<Label> labels = new HashSet<>();

        fileSpecificLabelSchemasCollection.forEach(s -> labels.addAll(s.labels()));

        Map<Label, MasterLabelSchema> masterLabelSchemas = new HashMap<>();

        for (Label label : labels) {

            LabelSchema masterLabelSchema = new LabelSchema(label);
            Collection<FileSpecificLabelSchema> fileSpecificLabelSchemas = new ArrayList<>();

            for (FileSpecificLabelSchemas fileSpecificLabelSchemasForTask : fileSpecificLabelSchemasCollection) {
                if (fileSpecificLabelSchemasForTask.hasSchemasForLabel(label)) {
                    for (FileSpecificLabelSchema fileSpecificLabelSchema :
                            fileSpecificLabelSchemasForTask.fileSpecificLabelSchemasFor(label)) {
                        masterLabelSchema = masterLabelSchema.union(fileSpecificLabelSchema.labelSchema());
                        fileSpecificLabelSchemas.add(fileSpecificLabelSchema);
                    }
                }
            }

            masterLabelSchemas.put(
                    label,
                    new MasterLabelSchema(masterLabelSchema, fileSpecificLabelSchemas));


        }

        return new MasterLabelSchemas(masterLabelSchemas);
    }

    private final Map<Label, MasterLabelSchema> masterLabelSchemas;

    public MasterLabelSchemas(Map<Label, MasterLabelSchema> masterLabelSchemas) {
        this.masterLabelSchemas = masterLabelSchemas;
    }

    public void updateGraphSchema(GraphSchema graphSchema, GraphElementType<?> graphElementType) {
        GraphElementSchemas graphElementSchemas = new GraphElementSchemas();
        for (MasterLabelSchema masterLabelSchema : masterLabelSchemas.values()) {
            graphElementSchemas.addLabelSchema(masterLabelSchema.labelSchema());
        }
        graphSchema.replace(graphElementType, graphElementSchemas);
    }

    public Collection<MasterLabelSchema> schemas() {
        return masterLabelSchemas.values();
    }
}
