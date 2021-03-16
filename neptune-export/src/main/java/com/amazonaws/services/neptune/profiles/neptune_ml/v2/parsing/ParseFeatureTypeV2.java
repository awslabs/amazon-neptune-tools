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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ErrorMessageHelper;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.FeatureTypeV2;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;

public class ParseFeatureTypeV2 {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseFeatureTypeV2(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public FeatureTypeV2 parseFeatureType() {
        if (json.has("type") && json.get("type").isTextual()) {
            String type = json.get("type").textValue();
            if (type.equals("numerical") || type.equals("category") || type.equals("auto") || type.equals("none")) {
                return FeatureTypeV2.valueOf(type);
            } else {
                throw ErrorMessageHelper.invalidFieldValue("type", type, context, Arrays.asList("numerical", "category", "auto", "none"));
            }
        }
        throw ErrorMessageHelper.errorParsingField("type", context, "one of the following values: 'numerical', 'category', 'auto', 'none'");
    }
}
