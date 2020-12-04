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

package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ParseEdgeType {

    private final JsonNode json;
    private final String description;

    public ParseEdgeType(JsonNode json, String description) {
        this.json = json;
        this.description = description;
    }

    public Label parseEdgeType() {
        if (json.has("edge") && json.path("edge").isArray()){
            ArrayNode array = (ArrayNode) json.get("edge");
            if (array.size() != 3){
                throw new IllegalArgumentException(String.format("Error parsing 'edge' field: expected an array with 3 values for %s", description));
            }
            return new Label(array.get(1).textValue(), array.get(0).textValue(), array.get(2).textValue());
        } else {
            throw new IllegalArgumentException(String.format("Error parsing 'edge' field: expected an array with 3 values for %s", description));
        }
    }
}
