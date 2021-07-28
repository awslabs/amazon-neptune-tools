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

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ParseRange {

    private final JsonNode json;
    private final String fieldName;
    private final ParsingContext context;

    public ParseRange(JsonNode json, String fieldName, ParsingContext context) {
        this.json = json;
        this.fieldName = fieldName;
        this.context = context;
    }

    public Range parseRange() {
        if (json.has(fieldName) && json.path(fieldName).isArray()) {
            ArrayNode rangeNode = (ArrayNode) json.path(fieldName);
            if (rangeNode.size() != 2) {
                throw error();
            }
            if (!rangeNode.get(0).isNumber() || !rangeNode.get(1).isNumber()) {
                throw error();
            }
            return new Range(rangeNode.get(0).numberValue(), rangeNode.get(1).numberValue());
        } else {
            throw error();
        }
    }

    private IllegalArgumentException error() {
        return (ErrorMessageHelper.errorParsingField(fieldName, context, "an array with 2 numeric values"));
    }
}
