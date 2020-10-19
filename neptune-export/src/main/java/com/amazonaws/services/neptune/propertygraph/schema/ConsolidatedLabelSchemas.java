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

public class ConsolidatedLabelSchemas {

    public static ConsolidatedLabelSchemas fromCollection(Collection<GraphElementSchemas> allMetadataFromTasks) {

        Set<String> labels = new HashSet<>();

        for (GraphElementSchemas metadataForLabelsFromTask : allMetadataFromTasks) {
            for (LabelSchema metadata : metadataForLabelsFromTask.allMetadata()) {
                labels.add(metadata.label());
            }
        }

        Map<String, ConsolidatedLabelSchema> consolidatedMetadataByLabel = new HashMap<>();

        for (String label : labels) {

            LabelSchema consolidatedMetadataForLabel = new LabelSchema(label);
            Collection<FileSpecificLabelSchema> metadataForAllFiles = new ArrayList<>();

            for (GraphElementSchemas metadataForLabelsFromTask : allMetadataFromTasks) {

                LabelSchema metadataForLabel = metadataForLabelsFromTask.getSchemaFor(label);
                Collection<String> outputIds = metadataForLabel.outputIds();

                if (!outputIds.isEmpty()) {
                    consolidatedMetadataForLabel = consolidatedMetadataForLabel.union(metadataForLabel);
                    for (String outputId : outputIds) {
                        metadataForAllFiles.add(new FileSpecificLabelSchema(outputId, metadataForLabel));
                    }
                }

            }

            consolidatedMetadataByLabel.put(
                    label,
                    new ConsolidatedLabelSchema(consolidatedMetadataForLabel, metadataForAllFiles));

        }

        return new ConsolidatedLabelSchemas(consolidatedMetadataByLabel);
    }


    private final Map<String, ConsolidatedLabelSchema> consolidatedMetadataForLabels;

    public ConsolidatedLabelSchemas(
            Map<String, ConsolidatedLabelSchema> consolidatedMetadataForLabels) {
        this.consolidatedMetadataForLabels = consolidatedMetadataForLabels;
    }
}
