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
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.*;

public class ParseEdgeType {

    private final JsonNode json;
    private final ParsingContext parsingContext;

    public ParseEdgeType(JsonNode json, ParsingContext parsingContext) {
        this.json = json;
        this.parsingContext = parsingContext;
    }

    public Label parseEdgeType() {
        if (json.has("edge") && json.path("edge").isArray()){
            ArrayNode array = (ArrayNode) json.get("edge");
            if (array.size() != 3){
                throw error();
            }
            List<String> fromLabels = getLabels(array.get(0));
            String edgeLabel = array.get(1).textValue();
            List<String> toLabels = getLabels(array.get(2));

            if (fromLabels.size() == 1 && toLabels.size() == 1){
                return new Label(edgeLabel, fromLabels.get(0), toLabels.get(0));
            } else {
                return new Label(edgeLabel, fromLabels, toLabels);
            }

        } else {
            throw error();
        }
    }

    private List<String> getLabels(JsonNode jsonNode){
        if (jsonNode.isTextual()){
            return Collections.singletonList(jsonNode.textValue());
        } else if (jsonNode.isArray()){
            List<String> values = new ArrayList<>();
            for (JsonNode element : jsonNode) {
                values.add(element.textValue());
            }
            return values;
        } else {
            return Collections.emptyList();
        }

    }

    private IllegalArgumentException error() {
        return ErrorMessageHelper.errorParsingField("edge", parsingContext, "an array with 3 values");
    }
}
