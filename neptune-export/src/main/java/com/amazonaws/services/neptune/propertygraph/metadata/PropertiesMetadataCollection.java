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

public class PropertiesMetadataCollection implements Jsonizable {

    public static PropertiesMetadataCollection fromJson(JsonNode json) {

        Map<GraphElementType<?>, PropertiesMetadata> metadataCollection = new HashMap<>();

        for (GraphElementType<?> graphElementType : MetadataTypes.values()) {
            JsonNode node = json.path(graphElementType.name());
            if (!node.isMissingNode() && node.isArray()) {
                metadataCollection.put(graphElementType, PropertiesMetadata.fromJson((ArrayNode) node));
            }
        }

        return new PropertiesMetadataCollection(metadataCollection);
    }

    private final Map<GraphElementType<?>, PropertiesMetadata> metadataCollection;

    public PropertiesMetadataCollection() {
        this(new HashMap<>());
    }

    public PropertiesMetadataCollection(Map<GraphElementType<?>, PropertiesMetadata> metadataCollection) {
        this.metadataCollection = metadataCollection;
    }

    public void update(GraphElementType<?> graphElementType, Map<?, Object> properties, boolean allowStructuralElements) {
        if (!metadataCollection.containsKey(graphElementType)) {
            metadataCollection.put(graphElementType, new PropertiesMetadata());
        }
        metadataCollection.get(graphElementType).update(properties, allowStructuralElements);
    }

    public PropertiesMetadata propertyMetadataFor(GraphElementType<?> type) {
        if (!metadataCollection.containsKey(type)) {
            metadataCollection.put(type, new PropertiesMetadata());
        }
        return metadataCollection.get(type);
    }

    @Override
    public JsonNode toJson() {
        ObjectNode json = JsonNodeFactory.instance.objectNode();

        for (Map.Entry<GraphElementType<?>, PropertiesMetadata> entry : metadataCollection.entrySet()) {
            String key = entry.getKey().name();
            ArrayNode arrayNode = entry.getValue().toJson();
            json.set(key, arrayNode);
        }
        return json;
    }
}
