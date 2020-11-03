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

package com.amazonaws.services.neptune.dgl.parsing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ParseCols {

    private final JsonNode json;
    private final String description;

    public ParseCols(JsonNode json, String description) {
        this.json = json;
        this.description = description;
    }

    public String parseSingleColumn() {
        if (json.has("cols")) {
            return getCol(json.path("cols"));
        } else {
            throw new IllegalArgumentException(String.format("Expected a 'cols' field for %s", description));
        }
    }

    private String getCol(JsonNode cols) {
        if (cols.isArray()) {
            ArrayNode colsArray = (ArrayNode) cols;
            if (colsArray.size() > 1) {
                throw new IllegalArgumentException(String.format("Error parsing 'cols' field: expected a text value or single-valued text array for %s", description));
            } else {
                return getColValue(colsArray.get(0));
            }
        } else {
            return getColValue(cols);
        }
    }

    private String getColValue(JsonNode col) {
        if (col.isTextual()) {
            return col.textValue();
        } else {
            throw new IllegalArgumentException(String.format("Error parsing 'cols' field: expected a text value or single-valued text array for %s", description));
        }
    }
}
