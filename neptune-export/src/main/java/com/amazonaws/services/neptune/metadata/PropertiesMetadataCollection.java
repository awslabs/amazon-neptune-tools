package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public class PropertiesMetadataCollection {

    public static PropertiesMetadataCollection fromJson(JsonNode json) {

        Map<MetadataType<?>, PropertiesMetadata> metadataCollection = new HashMap<>();

        for (MetadataType<?> metadataType : MetadataTypes.values()) {
            JsonNode node = json.path(metadataType.name());
            if (!node.isMissingNode() && node.isArray()) {
                metadataCollection.put(metadataType, PropertiesMetadata.fromJson((ArrayNode) node));
            }
        }

        return new PropertiesMetadataCollection(metadataCollection);
    }

    private final Map<MetadataType<?>, PropertiesMetadata> metadataCollection;

    public PropertiesMetadataCollection() {
        this(new HashMap<>());
    }

    public PropertiesMetadataCollection(Map<MetadataType<?>, PropertiesMetadata> metadataCollection) {
        this.metadataCollection = metadataCollection;
    }

    public void update(MetadataType<?> metadataType, Map<?, Object> properties, boolean allowStructuralElements) {
        if (!metadataCollection.containsKey(metadataType)) {
            metadataCollection.put(metadataType, new PropertiesMetadata());
        }
        metadataCollection.get(metadataType).update(properties, allowStructuralElements);
    }

    public PropertiesMetadata propertyMetadataFor(MetadataType type) {
        return metadataCollection.get(type);
    }

    public ObjectNode toJson() {
        ObjectNode json = JsonNodeFactory.instance.objectNode();

        for (Map.Entry<MetadataType<?>, PropertiesMetadata> entry : metadataCollection.entrySet()) {
            String key = entry.getKey().name();
            ArrayNode arrayNode = entry.getValue().toJson();
            json.set(key, arrayNode);
        }
        return json;
    }
}
