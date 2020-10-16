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

import com.amazonaws.services.neptune.propertygraph.io.Jsonizable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public class PropertyMetadataForGraph implements Jsonizable {

    public static PropertyMetadataForGraph fromJson(JsonNode json) {

        Map<GraphElementType<?>, PropertyMetadataForLabels> metadataCollection = new HashMap<>();

        for (GraphElementType<?> graphElementType : MetadataTypes.values()) {
            JsonNode node = json.path(graphElementType.name());
            if (!node.isMissingNode() && node.isArray()) {
                metadataCollection.put(graphElementType, PropertyMetadataForLabels.fromJson((ArrayNode) node));
            }
        }

        return new PropertyMetadataForGraph(metadataCollection);
    }

    private final Map<GraphElementType<?>, PropertyMetadataForLabels> metadataCollection;

    public PropertyMetadataForGraph() {
        this(new HashMap<>());
    }

    public PropertyMetadataForGraph(Map<GraphElementType<?>, PropertyMetadataForLabels> metadataCollection) {
        this.metadataCollection = metadataCollection;
    }

    public void update(GraphElementType<?> graphElementType, Map<?, Object> properties, boolean allowStructuralElements) {
        if (!metadataCollection.containsKey(graphElementType)) {
            metadataCollection.put(graphElementType, new PropertyMetadataForLabels());
        }
        metadataCollection.get(graphElementType).update(properties, allowStructuralElements);
    }

    public PropertyMetadataForLabels propertyMetadataFor(GraphElementType<?> type) {
        if (!metadataCollection.containsKey(type)) {
            metadataCollection.put(type, new PropertyMetadataForLabels());
        }
        return metadataCollection.get(type);
    }

    @Override
    public JsonNode toJson() {
        ObjectNode json = JsonNodeFactory.instance.objectNode();

        for (Map.Entry<GraphElementType<?>, PropertyMetadataForLabels> entry : metadataCollection.entrySet()) {
            String key = entry.getKey().name();
            ArrayNode arrayNode = entry.getValue().toJson();
            json.set(key, arrayNode);
        }
        return json;
    }
}
