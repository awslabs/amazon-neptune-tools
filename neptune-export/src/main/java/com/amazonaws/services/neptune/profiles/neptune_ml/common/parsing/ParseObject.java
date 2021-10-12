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

import com.fasterxml.jackson.databind.JsonNode;

public class ParseObject {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseObject(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public String parseObject() {
        if (json.has("object")) {
            JsonNode node = json.get("object");
            if (node.isTextual()) {
                return node.textValue();
            } else {
                throw error();
            }
        } else {
            return null;
        }
    }

    private IllegalArgumentException error() {
        return ErrorMessageHelper.errorParsingField("object", context, "a text value");
    }
}
