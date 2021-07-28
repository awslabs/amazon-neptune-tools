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
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

public class ParseSplitRate {
    private final JsonNode json;
    private final Collection<Double> defaultSplitRates;
    private final ParsingContext context;

    public ParseSplitRate(JsonNode json, Collection<Double> defaultSplitRates, ParsingContext context) {
        this.json = json;
        this.defaultSplitRates = defaultSplitRates;
        this.context = context;
    }

    public Collection<Double> parseSplitRates() {

        if (json.has("split_rate")){
            if (json.get("split_rate").isArray()){
                ArrayNode splitRatesArray = (ArrayNode) json.get("split_rate");
                if (splitRatesArray.size() == 3) {
                    Collection<Double> splitRates = new ArrayList<>();
                    for (JsonNode jsonNode : splitRatesArray) {
                        if (jsonNode.isDouble()) {
                            splitRates.add(jsonNode.asDouble());
                        } else {
                            throw error();
                        }
                    }
                    Optional<Double> sum = splitRates.stream().reduce(Double::sum);
                    if (sum.orElse(0.0) != 1.0) {
                        throw error();
                    }
                    return splitRates;
                } else {
                    throw error();
                }
            } else {
                throw error();
            }
        } else {
            return defaultSplitRates;
        }
    }

    private IllegalArgumentException error(){
        return ErrorMessageHelper.errorParsingField("split_rate", context, "an array with 3 double values that add up to 1.0");
    }
}
