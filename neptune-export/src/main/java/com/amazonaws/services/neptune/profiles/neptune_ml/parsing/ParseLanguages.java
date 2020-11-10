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
import java.util.Collection;

public class ParseLanguages {
    private final JsonNode json;
    private final String description;

    public ParseLanguages(JsonNode json, String description) {

        this.json = json;
        this.description = description;
    }

    public Collection<String> parseLanguages() {
        if (json.has("language") && json.path("language").isArray()) {
            Collection<String> languages = new ArrayList<>();
            ArrayNode language = (ArrayNode) json.path("language");
            for (JsonNode node : language) {
                languages.add(node.textValue());
            }
            return languages;
        } else {
            throw new IllegalArgumentException(String.format("Error parsing 'language' field: expected a text array value for %s", description));
        }
    }
}
