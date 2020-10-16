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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.Map;

public class PropertyMetadataForLabels {

    public static PropertyMetadataForLabels fromJson(ArrayNode arrayNode) {
        Map<String, PropertyMetadataForLabel> metadata = new HashMap<>();

        for (JsonNode node : arrayNode) {
            String label = node.path("label").asText();

            metadata.put(label, new PropertyMetadataForLabel());
            ArrayNode propertiesArray = (ArrayNode) node.path("properties");

            for (JsonNode propertyNode : propertiesArray) {

                String key = propertyNode.path("property").textValue();

                DataType dataType = Enum.valueOf(DataType.class, propertyNode.path("dataType").textValue());
                boolean isMultiValue = propertyNode.path("isMultiValue").booleanValue();

                metadata.get(label).put(key, new PropertyTypeInfo(key, dataType, isMultiValue));
            }
        }
        return new PropertyMetadataForLabels(metadata);
    }

    private final Map<String, PropertyMetadataForLabel> metadataByLabel;

    public PropertyMetadataForLabels() {
        this(new HashMap<>());
    }

    private PropertyMetadataForLabels(Map<String, PropertyMetadataForLabel> metadataByLabel) {
        this.metadataByLabel = metadataByLabel;
    }

    public PropertyMetadataForLabel getMetadataFor(String label) {

        if (!metadataByLabel.containsKey(label)) {
            metadataByLabel.put(label, new PropertyMetadataForLabel());
        }

        return metadataByLabel.get(label);
    }

    public boolean hasMetadataFor(String label) {
        return metadataByLabel.containsKey(label);
    }

    public void update(Map<?, ?> properties, boolean allowStructuralElements) {

        String label = String.valueOf(properties.get(org.apache.tinkerpop.gremlin.structure.T.label));

        update(label, properties, allowStructuralElements);
    }

    public void update(String label, Map<?, ?> properties, boolean allowStructuralElements) {

        if (!metadataByLabel.containsKey(label)) {
            metadataByLabel.put(label, new PropertyMetadataForLabel());
        }

        PropertyMetadataForLabel propertyMetadataForLabel = metadataByLabel.get(label);

        for (Map.Entry<?, ?> entry : properties.entrySet()) {

            Object property = entry.getKey();

            if (allowStructuralElements || !(isToken(property))) {
                if (!propertyMetadataForLabel.containsProperty(property)) {
                    propertyMetadataForLabel.put(property, new PropertyTypeInfo(property));
                }
                propertyMetadataForLabel.getPropertyTypeInfo(property).accept(entry.getValue());
            }
        }
    }

    public Iterable<String> labels() {
        return metadataByLabel.keySet();
    }

    private boolean isToken(Object key) {
        return key.equals(T.label) || key.equals(T.id) || key.equals(T.key) || key.equals(T.value);
    }

    public ArrayNode toJson() {

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

        for (Map.Entry<String, PropertyMetadataForLabel> entry : metadataByLabel.entrySet()) {

            String label = entry.getKey();

            ObjectNode labelNode = JsonNodeFactory.instance.objectNode();
            labelNode.put("label", label);

            ArrayNode propertiesNode = JsonNodeFactory.instance.arrayNode();

            PropertyMetadataForLabel propertyMetadataForLabel = entry.getValue();

            for (PropertyTypeInfo propertyTypeInfo : propertyMetadataForLabel.properties()) {

                ObjectNode propertyNode = JsonNodeFactory.instance.objectNode();
                propertyNode.put("property", propertyTypeInfo.property().toString());
                propertyNode.put("dataType", propertyTypeInfo.dataType().name());
                propertyNode.put("isMultiValue", propertyTypeInfo.isMultiValue());
                propertiesNode.add(propertyNode);
            }

            labelNode.set("properties", propertiesNode);

            arrayNode.add(labelNode);
        }

        return arrayNode;
    }

    public PropertyMetadataForLabels createCopy() {
        return fromJson(toJson());
    }
}
