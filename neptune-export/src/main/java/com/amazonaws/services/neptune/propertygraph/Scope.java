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

import com.amazonaws.services.neptune.propertygraph.metadata.MetadataSpecification;
import com.amazonaws.services.neptune.propertygraph.metadata.MetadataTypes;

import java.util.*;
import java.util.stream.Collectors;

public enum Scope {
    all {
        @Override
        public Collection<MetadataSpecification<?>> metadataSpecifications(List<String> nodeLabels,
                                                                           List<String> edgeLabels) {
            return Arrays.asList(
                    new MetadataSpecification<>(MetadataTypes.Nodes, Scope.labelsFilter(nodeLabels)),
                    new MetadataSpecification<>(MetadataTypes.Edges, Scope.labelsFilter(edgeLabels))
            );
        }
    },
    nodes {
        @Override
        public Collection<MetadataSpecification<?>> metadataSpecifications(List<String> nodeLabels,
                                                                           List<String> edgeLabels) {
            return Collections.singletonList(
                    new MetadataSpecification<>(MetadataTypes.Nodes, Scope.labelsFilter(nodeLabels))
            );
        }
    },
    edges {
        @Override
        public Collection<MetadataSpecification<?>> metadataSpecifications(List<String> nodeLabels,
                                                                           List<String> edgeLabels) {
            return Collections.singletonList(
                    new MetadataSpecification<>(MetadataTypes.Edges, Scope.labelsFilter(edgeLabels))
            );
        }
    };

    private static Set<String> toSet(Collection<String> labels) {
        return labels.stream().flatMap(v -> Arrays.stream(v.split(","))).collect(Collectors.toSet());
    }

    private static LabelsFilter labelsFilter(Collection<String> labels){
        if (labels.isEmpty()){
            return AllLabels.INSTANCE;
        }

        return new SpecifiedLabels(toSet(labels));
    }

    public abstract Collection<MetadataSpecification<?>> metadataSpecifications(
            List<String> nodeLabels,
            List<String> edgeLabels);

}
