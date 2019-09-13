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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class PropertiesMetadata {

    public static PropertiesMetadata fromJson(ArrayNode arrayNode) {
        Map<String, Map<Object, PropertyTypeInfo>> metadata = new HashMap<>();

        for (JsonNode node : arrayNode) {
            String label = node.path("label").asText();

            metadata.put(label, new HashMap<>());
            ArrayNode propertiesArray = (ArrayNode) node.path("properties");

            for (JsonNode propertyNode : propertiesArray) {

                String key = propertyNode.path("property").textValue();

                DataType dataType = Enum.valueOf(DataType.class, propertyNode.path("dataType").textValue());
                boolean isMultiValue = propertyNode.path("isMultiValue").booleanValue();

                metadata.get(label).put(key, new PropertyTypeInfo(key, dataType, isMultiValue));
            }
        }
        return new PropertiesMetadata(metadata);
    }

    private final Map<String, Map<Object, PropertyTypeInfo>> metadata;

    public PropertiesMetadata() {
        this(new HashMap<>());
    }

    public PropertiesMetadata(Map<String, Map<Object, PropertyTypeInfo>> metadata) {
        this.metadata = metadata;
    }

    public Map<Object, PropertyTypeInfo> propertyMetadataFor(String label) {

        if (!metadata.containsKey(label)) {
            metadata.put(label, new LinkedHashMap<>());
        }

        return metadata.get(label);
    }

    public boolean hasMetadataFor(String label){
        return metadata.containsKey(label);
    }

    public void update(Map<?, ?> properties, boolean allowStructuralElements) {

        String label = String.valueOf(properties.get(org.apache.tinkerpop.gremlin.structure.T.label));

        update(label, properties, allowStructuralElements);
    }

    public void update(String label, Map<?, ?> properties, boolean allowStructuralElements) {

        if (!metadata.containsKey(label)) {
            metadata.put(label, new LinkedHashMap<>());
        }

        Map<Object, PropertyTypeInfo> propertyInfo = metadata.get(label);

        for (Map.Entry<?, ?> entry : properties.entrySet()) {

            Object property = entry.getKey();

            if (allowStructuralElements || !(isToken(property))){
                if (!propertyInfo.containsKey(property)) {
                    propertyInfo.put(property, new PropertyTypeInfo(property));
                }
                propertyInfo.get(property).accept(entry.getValue());
            }
        }
    }

    public Iterable<String> labels(){
        return metadata.keySet();
    }

    private boolean isToken(Object key) {
        return key.equals(T.label) || key.equals(T.id) || key.equals(T.key) || key.equals(T.value);
    }

    public ArrayNode toJson() {

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

        for (Map.Entry<String, Map<Object, PropertyTypeInfo>> entry : metadata.entrySet()) {

            String label = entry.getKey();

            ObjectNode labelNode = JsonNodeFactory.instance.objectNode();
            labelNode.put("label", label);

            ArrayNode propertiesNode = JsonNodeFactory.instance.arrayNode();

            Map<Object, PropertyTypeInfo> properties = entry.getValue();
            for (Map.Entry<Object, PropertyTypeInfo> property : properties.entrySet()) {
                PropertyTypeInfo typeInfo = property.getValue();

                ObjectNode propertyNode = JsonNodeFactory.instance.objectNode();
                propertyNode.put("property", property.getKey().toString());
                propertyNode.put("dataType", typeInfo.dataType().name());
                propertyNode.put("isMultiValue", typeInfo.isMultiValue());
                propertiesNode.add(propertyNode);
            }

            labelNode.set("properties", propertiesNode);

            arrayNode.add(labelNode);
        }

        return arrayNode;
    }
}
