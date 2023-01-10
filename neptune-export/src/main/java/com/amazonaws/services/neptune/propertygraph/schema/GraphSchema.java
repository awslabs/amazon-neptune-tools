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

package com.amazonaws.services.neptune.propertygraph.schema;

import com.amazonaws.services.neptune.propertygraph.io.Jsonizable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GraphSchema implements Jsonizable<Boolean> {

    public static GraphSchema fromJson(JsonNode json) {

        Map<GraphElementType, GraphElementSchemas> graphElementsSchemas = new HashMap<>();

        for (GraphElementType graphElementType : GraphElementType.values()) {
            JsonNode node = json.path(graphElementType.name());
            if (!node.isMissingNode() && node.isArray()) {
                graphElementsSchemas.put(graphElementType, GraphElementSchemas.fromJson((ArrayNode) node));
            }
        }

        return new GraphSchema(graphElementsSchemas);
    }

    private final Map<GraphElementType, GraphElementSchemas> graphElementsSchemas;

    public GraphSchema() {
        this(new HashMap<>());
    }

    public GraphSchema(Map<GraphElementType, GraphElementSchemas> graphElementsSchemas) {
        this.graphElementsSchemas = graphElementsSchemas;
    }

    public void update(GraphElementType graphElementType, Map<?, Object> properties, boolean allowStructuralElements) {
        graphElementSchemasFor(graphElementType).update(properties, allowStructuralElements);
    }

    public GraphElementSchemas copyOfGraphElementSchemasFor(GraphElementType graphElementType) {
        return graphElementSchemasFor(graphElementType).createCopy();
    }

    public GraphElementSchemas graphElementSchemasFor(GraphElementType graphElementType) {
        if (!graphElementsSchemas.containsKey(graphElementType)) {
            graphElementsSchemas.put(graphElementType, new GraphElementSchemas());
        }
        return graphElementsSchemas.get(graphElementType);
    }

    public Collection<GraphElementSchemas> graphElementSchemas() {
        return graphElementsSchemas.values();
    }

    public boolean isEmpty() {
        return graphElementsSchemas.isEmpty();
    }

    public boolean hasNodeSchemas() {
        return graphElementsSchemas.containsKey(GraphElementType.nodes);
    }

    public boolean hasEdgeSchemas() {
        return graphElementsSchemas.containsKey(GraphElementType.edges);
    }

    @Override
    public JsonNode toJson(Boolean includeFilenames) {
        ObjectNode json = JsonNodeFactory.instance.objectNode();

        for (Map.Entry<GraphElementType, GraphElementSchemas> entry : graphElementsSchemas.entrySet()) {
            String key = entry.getKey().name();
            ArrayNode arrayNode = entry.getValue().toJson(includeFilenames);
            json.set(key, arrayNode);
        }
        return json;
    }
}
