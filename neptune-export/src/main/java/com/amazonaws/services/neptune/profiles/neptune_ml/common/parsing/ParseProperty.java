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

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class ParseProperty {

    private final JsonNode json;
    private final ParsingContext context;
    private final NeptuneMLSourceDataModel dataModel;

    public ParseProperty(JsonNode json, ParsingContext context, NeptuneMLSourceDataModel dataModel) {
        this.json = json;
        this.context = context;
        this.dataModel = dataModel;
    }

    public ParseProperty(JsonNode json, ParsingContext context) {
        this(json, context, NeptuneMLSourceDataModel.PropertyGraph);
    }

    public String parseSingleProperty() {
        String fieldName = dataModel.nodeAttributeNameSingular().toLowerCase();

        if (json.has(fieldName) && json.get(fieldName).isTextual()) {
            return json.get(fieldName).textValue();
        } else {
            throw ErrorMessageHelper.errorParsingField(fieldName, context, String.format("a '%s' field with a string value", fieldName));
        }
    }

    public String parseNullableSingleProperty() {
        String fieldName = dataModel.nodeAttributeNameSingular().toLowerCase();
        if (json.has(fieldName) && json.get(fieldName).isTextual()) {
            return json.get(fieldName).textValue();
        } else {
            return "";
        }
    }

    public Collection<String> parseMultipleProperties() {
        String fieldNameSingular = dataModel.nodeAttributeNameSingular().toLowerCase();
        String fieldNamePlural = dataModel.nodeAttributeNamePlural().toLowerCase();

        if (json.has(fieldNameSingular) && json.get(fieldNameSingular).isTextual()) {
            return Collections.singletonList(json.get(fieldNameSingular).textValue());
        }
        if (json.has(fieldNamePlural) && json.get(fieldNamePlural).isArray()) {
            ArrayNode properties = (ArrayNode) json.get(fieldNamePlural);
            Collection<String> results = new ArrayList<>();
            for (JsonNode property : properties) {
                results.add(property.textValue());
            }
            return results;
        } else {
            throw new IllegalArgumentException(String.format("Expected a '%s' field with a string value, or a '%s' field with an array value for %s.",  fieldNameSingular, fieldNamePlural, context));
        }
    }
}
