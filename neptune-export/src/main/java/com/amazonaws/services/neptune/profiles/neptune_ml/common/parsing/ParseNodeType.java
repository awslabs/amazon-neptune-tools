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

import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class ParseNodeType {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseNodeType(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public Label parseNodeType(){
        if (json.has("node")){
            JsonNode node = json.get("node");
            if (node.isTextual()){
                return new Label(node.textValue());
            } else if (node.isArray()){
                Collection<String> values = new ArrayList<>();
                for (JsonNode element : node) {
                    values.add(element.textValue());
                }
                return new Label(values);
            } else {
                throw error();
            }
        } else {
            throw error();
        }
    }

    private IllegalArgumentException error() {
        return ErrorMessageHelper.errorParsingField("node", context, "a text value or array of text values");
    }
}
