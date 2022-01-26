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

package com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.SupportedWord2VecLanguages;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class ParseWord2VecLanguage {
    private final JsonNode json;

    public ParseWord2VecLanguage(JsonNode json) {
        this.json = json;
    }

    public Collection<String> parseLanguage() {
        Collection<String> results = new ArrayList<>();
        if (json.has("language")) {
            if (json.get("language").isArray()) {
                JsonNode arrayNode = json.get("language");
                for (JsonNode jsonNode : arrayNode) {
                    results.add(jsonNode.textValue());
                }
            } else if (json.get("language").isTextual()) {
                results.add(json.get("language").textValue());
            }
        }
        if (results.isEmpty()) {
            results.add(SupportedWord2VecLanguages.en_core_web_lg.name());
        }
        return results;
    }
}
