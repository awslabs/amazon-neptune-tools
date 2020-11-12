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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class ParseProperty {

    private final JsonNode json;
    private final String description;

    public ParseProperty(JsonNode json, String description) {
        this.json = json;
        this.description = description;
    }

    public String parseSingleProperty() {
        if (json.has("property") && json.get("property").isTextual()) {
            return json.get("property").textValue();
        } else {
            throw new IllegalArgumentException(String.format("Expected a 'property' field with a string value for %s", description));
        }
    }

    public Collection<String> parseMultipleProperties() {
        if (json.has("property") && json.get("property").isTextual()) {
            return Collections.singletonList(json.get("property").textValue());
        } if (json.has("properties") && json.get("properties").isArray()){
            ArrayNode properties = (ArrayNode) json.get("properties");
            Collection<String> results = new ArrayList<>();
            for (JsonNode property : properties) {
                results.add(property.textValue());
            }
            return results;
        } else {
            throw new IllegalArgumentException(String.format("Expected a 'property' field with a string value, or a 'properties' field with an array value for %s", description));
        }
    }
}
