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

import com.amazonaws.services.neptune.propertygraph.metadata.PropertiesMetadata;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AllLabels implements LabelsFilter {

    static final LabelsFilter INSTANCE = new AllLabels();

    private AllLabels(){}

    @Override
    public GraphTraversal<? extends Element, ?> apply(GraphTraversal<? extends Element, ?> traversal) {
        return traversal;
    }

    @Override
    public Collection<String> resolveLabels(GraphClient<?> graphClient) {
        return graphClient.labels();
    }

    @Override
    public String[] getPropertiesForLabels(PropertiesMetadata propertiesMetadata) {

        Set<String> properties = new HashSet<>();

        Iterable<String> labels = propertiesMetadata.labels();

        for (String label : labels) {
            Map<Object, PropertyTypeInfo> metadata = propertiesMetadata.propertyMetadataFor(label);
            for (PropertyTypeInfo propertyTypeInfo : metadata.values()) {
                properties.add(propertyTypeInfo.nameWithoutDataType());
            }
        }

        return properties.toArray(new String[]{});
    }
}
