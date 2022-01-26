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
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.SupportedFastTextLanguages;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;

public class ParseFastTextLanguage {

    private final JsonNode json;
    private final ParsingContext context;


    public ParseFastTextLanguage(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public String parseLanguage() {

        if (json.has("language")) {
            if (json.get("language").isTextual()) {
                return json.get("language").textValue();
            }
        }

        throw ErrorMessageHelper.errorParsingField(
                "language",
                context,
                "one of the following values: " + ErrorMessageHelper.quoteList(Arrays.asList(SupportedFastTextLanguages.values())));

    }

}
