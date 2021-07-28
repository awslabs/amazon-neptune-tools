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
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.DatetimePartV2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class ParseDatetimePartsV2 {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseDatetimePartsV2(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public Collection<DatetimePartV2> parseDatetimeParts() {
        if (json.has("datetime_parts")) {
            if (json.get("datetime_parts").isArray()) {
                ArrayNode datetimeParts = (ArrayNode) json.get("datetime_parts");
                Collection<DatetimePartV2> results = new ArrayList<>();
                for (JsonNode datetimePart : datetimeParts) {
                    String value = datetimePart.textValue();
                    try {
                        results.add(DatetimePartV2.valueOf(value));
                    } catch (IllegalArgumentException e) {
                        throw ErrorMessageHelper.invalidFieldValue("datetime_parts", value, context, datetimePartNames());
                    }
                }
                return results.isEmpty() ? Arrays.asList(DatetimePartV2.values()) : results;

            } else {
                throw ErrorMessageHelper.errorParsingField("datetime_parts", context, "an array value");
            }

        } else {
            return Arrays.asList(DatetimePartV2.values());
        }
    }

    private Collection<String> datetimePartNames() {
        Collection<String> results = new ArrayList<>();
        for (DatetimePartV2 value : DatetimePartV2.values()) {
            results.add(value.name());
        }
        return results;
    }
}
