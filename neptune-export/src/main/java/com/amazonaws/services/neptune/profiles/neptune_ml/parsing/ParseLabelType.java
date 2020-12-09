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

package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.fasterxml.jackson.databind.JsonNode;

public class ParseLabelType {

    private final String prefix;
    private final JsonNode json;

    public ParseLabelType(String prefix, JsonNode json) {
        this.prefix = prefix;
        this.json = json;
    }

    public String parseLabel() {
        if (json.has("type") && json.get("type").isTextual()) {
            String type = json.get("type").textValue();
            if  ( type.equals("regression")){
                return regressionLabel();
            } else if (type.equals("classification")){
                return classLabel();
            } else {
                throw new IllegalArgumentException(String.format("Invalid value for 'type' field: '%s'. Valid values are 'classification' and 'regression'.", type));
            }
        }

        return classLabel();
    }

    private String regressionLabel() {
        return String.format("%s_regression_label", prefix);
    }

    private String classLabel() {
        return String.format("%s_class_label", prefix);
    }
}
