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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class GraphElementSchemas {

    public static GraphElementSchemas fromJson(ArrayNode arrayNode) {
        Map<String, LabelSchema> labelSchemas = new HashMap<>();

        for (JsonNode node : arrayNode) {
            String label = node.path("label").asText();

            labelSchemas.put(label, new LabelSchema(label));
            ArrayNode propertiesArray = (ArrayNode) node.path("properties");

            for (JsonNode propertyNode : propertiesArray) {

                String key = propertyNode.path("property").textValue();

                DataType dataType = Enum.valueOf(DataType.class, propertyNode.path("dataType").textValue());
                boolean isMultiValue = propertyNode.path("isMultiValue").booleanValue();
                boolean isNullable = propertyNode.has("isNullable") ?
                        propertyNode.path("isNullable").booleanValue() :
                        false;

                labelSchemas.get(label).put(key, new PropertySchema(key, isNullable, dataType, isMultiValue));
            }
        }
        return new GraphElementSchemas(labelSchemas);
    }

    private final Map<String, LabelSchema> labelSchemas;

    public GraphElementSchemas() {
        this(new HashMap<>());
    }

    private GraphElementSchemas(Map<String, LabelSchema> labelSchemas) {
        this.labelSchemas = labelSchemas;
    }

    public void addLabelSchema(LabelSchema labelSchema){
        labelSchemas.put(labelSchema.label(), labelSchema);
    }

    public LabelSchema getSchemaFor(String label) {

        if (!labelSchemas.containsKey(label)) {
            labelSchemas.put(label, new LabelSchema(label));
        }

        return labelSchemas.get(label);
    }

    public boolean hasSchemaFor(String label) {
        return labelSchemas.containsKey(label);
    }

    public void update(Map<?, ?> properties, boolean allowStructuralElements) {

        String label = String.valueOf(properties.get(org.apache.tinkerpop.gremlin.structure.T.label));

        update(label, properties, allowStructuralElements);
    }

    public void update(String label, Map<?, ?> properties, boolean allowStructuralElements) {

        if (!labelSchemas.containsKey(label)) {
            labelSchemas.put(label, new LabelSchema(label));
        }

        LabelSchema labelSchema = labelSchemas.get(label);

        for (Map.Entry<?, ?> entry : properties.entrySet()) {

            Object property = entry.getKey();

            if (allowStructuralElements || !(isToken(property))) {
                if (!labelSchema.containsProperty(property)) {
                    labelSchema.put(property, new PropertySchema(property));
                }
                labelSchema.getPropertySchema(property).accept(entry.getValue());
            }
        }
    }

    public Iterable<String> labels() {
        return labelSchemas.keySet();
    }

    private boolean isToken(Object key) {
        return key.equals(T.label) || key.equals(T.id) || key.equals(T.key) || key.equals(T.value);
    }

    public ArrayNode toJson() {

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();

        for (Map.Entry<String, LabelSchema> entry : labelSchemas.entrySet()) {

            String label = entry.getKey();

            ObjectNode labelNode = JsonNodeFactory.instance.objectNode();
            labelNode.put("label", label);

            ArrayNode propertiesNode = JsonNodeFactory.instance.arrayNode();

            LabelSchema labelSchema = entry.getValue();

            for (PropertySchema propertySchema : labelSchema.propertySchemas()) {

                ObjectNode propertyNode = JsonNodeFactory.instance.objectNode();
                propertyNode.put("property", propertySchema.property().toString());
                propertyNode.put("dataType", propertySchema.dataType().name());
                propertyNode.put("isMultiValue", propertySchema.isMultiValue());
                propertyNode.put("isNullable", propertySchema.isNullable());
                propertiesNode.add(propertyNode);
            }

            labelNode.set("properties", propertiesNode);

            arrayNode.add(labelNode);
        }

        return arrayNode;
    }

    public GraphElementSchemas createCopy() {
        return fromJson(toJson());
    }
}
