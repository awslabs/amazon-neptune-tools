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

import com.amazonaws.services.neptune.profiles.neptune_ml.TrainingJobWriterConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ParseRange {

    private final JsonNode json;
    private final String description;

    public ParseRange(JsonNode json, String description) {
        this.json = json;
        this.description = description;
    }

    public TrainingJobWriterConfig.Range parseRange() {
        if (json.has("range") && json.path("range").isArray()) {
            ArrayNode rangeNode = (ArrayNode) json.path("range");
            if (rangeNode.size() != 2) {
                throwError();
            }
            if (!rangeNode.get(0).isNumber() || !rangeNode.get(1).isNumber()) {
                throwError();
            }
            return new TrainingJobWriterConfig.Range(rangeNode.get(0).numberValue(), rangeNode.get(1).numberValue());
        } else {
            return throwError();
        }
    }

    private TrainingJobWriterConfig.Range throwError() {
        throw new IllegalArgumentException(String.format("Error parsing 'range' field: expected an array with 2 numeric values for %s", description));
    }
}
