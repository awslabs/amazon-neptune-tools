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

import java.util.*;

public class MasterLabelSchemas {

    public static MasterLabelSchemas fromCollection(Collection<FileSpecificLabelSchemas> allFileSpecificLabelSchemas) {

        Set<String> labels = new HashSet<>();

        allFileSpecificLabelSchemas.forEach(s -> labels.addAll(s.labels()));

        Map<String, MasterLabelSchema> masterLabelSchemas = new HashMap<>();

        for (String label : labels) {

            LabelSchema masterLabelSchema = new LabelSchema(label);
            Collection<FileSpecificLabelSchema> fileSpecificLabelSchemas = new ArrayList<>();

            for (FileSpecificLabelSchemas fileSpecificLabelSchemasForTask : allFileSpecificLabelSchemas) {
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

    private final Map<String, MasterLabelSchema> masterLabelSchemas;

    public MasterLabelSchemas(Map<String, MasterLabelSchema> masterLabelSchemas) {
        this.masterLabelSchemas = masterLabelSchemas;
    }

    public Collection<MasterLabelSchema> masterSchemas(){
        return masterLabelSchemas.values();
    }
}
