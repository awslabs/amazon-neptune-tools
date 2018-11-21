package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.Map;

public class PropertiesMetadata {

    public static PropertiesMetadata fromJson(ArrayNode arrayNode) {
        Map<String, Map<String, PropertyTypeInfo>> metadata = new HashMap<>();

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

    private final Map<String, Map<String, PropertyTypeInfo>> metadata;

    public PropertiesMetadata() {
        this(new HashMap<>());
    }

    public PropertiesMetadata(Map<String, Map<String, PropertyTypeInfo>> metadata) {
        this.metadata = metadata;
    }

    public Map<String, PropertyTypeInfo> propertyMetadataFor(String label) {
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
            metadata.put(label, new HashMap<>());
        }

        Map<String, PropertyTypeInfo> propertyInfo = metadata.get(label);

        for (Map.Entry<?, ?> entry : properties.entrySet()) {

            Object key = entry.getKey();

            if (allowStructuralElements || !(isStructuralElement(key))){
                String property = propertyName(key);
                if (!propertyInfo.containsKey(property)) {
                    propertyInfo.put(property, new PropertyTypeInfo(property));
                }
                propertyInfo.get(property).accept(entry.getValue());
            }
        }
    }

    private boolean isStructuralElement(Object key) {
        return key.equals(T.label) || key.equals(T.id) || key.equals(T.key) || key.equals(T.value);
    }

    private String propertyName(Object key) {
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.label)) {
            return "~label";
        }
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.id)) {
            return "~id";
        }
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.key)) {
            return "~key";
        }
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.value)) {
            return "~value";
        }
        return String.valueOf(key);
    }

    public ArrayNode toJson() {

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

        for (Map.Entry<String, Map<String, PropertyTypeInfo>> entry : metadata.entrySet()) {

            String label = entry.getKey();

            ObjectNode labelNode = JsonNodeFactory.instance.objectNode();
            labelNode.put("label", label);

            ArrayNode propertiesNode = JsonNodeFactory.instance.arrayNode();

            Map<String, PropertyTypeInfo> properties = entry.getValue();
            for (Map.Entry<String, PropertyTypeInfo> property : properties.entrySet()) {
                PropertyTypeInfo typeInfo = property.getValue();

                ObjectNode propertyNode = JsonNodeFactory.instance.objectNode();
                propertyNode.put("property", property.getKey());
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
