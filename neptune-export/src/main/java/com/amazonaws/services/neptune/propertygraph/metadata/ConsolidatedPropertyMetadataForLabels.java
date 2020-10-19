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

package com.amazonaws.services.neptune.propertygraph.metadata;

import java.util.*;

public class ConsolidatedPropertyMetadataForLabels {

    public static ConsolidatedPropertyMetadataForLabels fromCollection(Collection<PropertyMetadataForLabels> allMetadataFromTasks) {

        Set<String> labels = new HashSet<>();

        for (PropertyMetadataForLabels metadataForLabelsFromTask : allMetadataFromTasks) {
            for (PropertyMetadataForLabel metadata : metadataForLabelsFromTask.allMetadata()) {
                labels.add(metadata.label());
            }
        }

        Map<String, ConsolidatedPropertyMetadataForLabel> consolidatedMetadataByLabel = new HashMap<>();

        for (String label : labels) {

            PropertyMetadataForLabel consolidatedMetadataForLabel = new PropertyMetadataForLabel(label);
            Collection<FileSpecificPropertyMetadata> metadataForAllFiles = new ArrayList<>();

            for (PropertyMetadataForLabels metadataForLabelsFromTask : allMetadataFromTasks) {

                PropertyMetadataForLabel metadataForLabel = metadataForLabelsFromTask.getMetadataFor(label);
                Collection<String> outputIds = metadataForLabel.outputIds();

                if (!outputIds.isEmpty()) {
                    consolidatedMetadataForLabel = consolidatedMetadataForLabel.union(metadataForLabel);
                    for (String outputId : outputIds) {
                        metadataForAllFiles.add(new FileSpecificPropertyMetadata(outputId, metadataForLabel));
                    }
                }

            }

            consolidatedMetadataByLabel.put(
                    label,
                    new ConsolidatedPropertyMetadataForLabel(consolidatedMetadataForLabel, metadataForAllFiles));

        }

        return new ConsolidatedPropertyMetadataForLabels(consolidatedMetadataByLabel);
    }


    private final Map<String, ConsolidatedPropertyMetadataForLabel> consolidatedMetadataForLabels;

    public ConsolidatedPropertyMetadataForLabels(
            Map<String, ConsolidatedPropertyMetadataForLabel> consolidatedMetadataForLabels) {
        this.consolidatedMetadataForLabels = consolidatedMetadataForLabels;
    }
}
