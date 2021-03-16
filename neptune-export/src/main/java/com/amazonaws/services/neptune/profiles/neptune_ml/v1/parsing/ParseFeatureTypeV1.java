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

package com.amazonaws.services.neptune.profiles.neptune_ml.v1.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ErrorMessageHelper;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.FeatureTypeV1;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;

public class ParseFeatureTypeV1 {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseFeatureTypeV1(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public FeatureTypeV1 parseFeatureType() {
        if (json.has("type") && json.get("type").isTextual()) {
            String type = json.get("type").textValue();
            if  ( type.equals("numerical") || type.equals("category")){
                return FeatureTypeV1.valueOf(type);
            } else {
                throw ErrorMessageHelper.invalidFieldValue("type", type, context, Arrays.asList("numerical", "category"));
            }
        }
        throw ErrorMessageHelper.errorParsingField("type", context, "'numerical' or 'category' value");
    }
}
