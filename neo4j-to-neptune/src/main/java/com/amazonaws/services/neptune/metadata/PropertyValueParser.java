/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PropertyValueParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PropertyValueParserPolicy policy;
    private final String semicolonReplacement;
    private final boolean inferType;
    private final String listDelimiter;
    private final boolean appendListDelimiter;
    private final boolean inferDateTimes;

    public PropertyValueParser(PropertyValueParserPolicy policy, String semicolonReplacement, boolean inferType) {
        this(policy, semicolonReplacement, inferType, "|", false, false);
    }

    public PropertyValueParser(PropertyValueParserPolicy policy, String semicolonReplacement, boolean inferType,
            String listDelimiter, boolean appendListDelimiter, boolean inferDateTimes) {
        this.policy = policy;
        this.semicolonReplacement = semicolonReplacement;
        this.inferType = inferType;
        this.listDelimiter = listDelimiter;
        this.appendListDelimiter = appendListDelimiter;
        this.inferDateTimes = inferDateTimes;
    }

    public PropertyValue parse(String s){
        if (isArrayCandidate(s)){
            try {
                JsonNode jsonNode = MAPPER.readTree(s);
                if (isArray(jsonNode)){
                    return policy.handleArray(s, (ArrayNode) jsonNode, this);
                } else {
                    return stringValue(s);
                }
            } catch (JsonProcessingException e) {
                return stringValue(s);
            }
        } else {
            return stringValue(s);
        }
    }

    PropertyValue parseArrayValue(String s, ArrayNode arrayNode) {
        Set<String> values = new HashSet<>();
        for (JsonNode node : arrayNode) {
            values.add(node.asText().replace(";", semicolonReplacement));
        }
        if (values.size() < arrayNode.size()){
            policy.handleDuplicates(s, arrayNode, this);
        }
        return arrayValue(values);
    }

    PropertyValue parseArrayAsDelimitedString(ArrayNode arrayNode) {
        List<String> values = new ArrayList<>();
        for (JsonNode node : arrayNode) {
            String value = node.asText();
            if (value.contains(listDelimiter)) {
                throw new IllegalArgumentException(String.format(
                    "List value contains configured delimiter '%s': %s", listDelimiter, value));
            }
            values.add(value);
        }

        String value = String.join(listDelimiter, values);
        if (appendListDelimiter && !values.isEmpty()) {
            value += listDelimiter;
        }

        DataType dataType = inferType ? DataType.String : DataType.None;
        return new PropertyValue(format(value), false, dataType);
    }

    PropertyValue stringValue(String s){
        DataType dataType = inferType ? DataType.identifyType(s, inferDateTimes) : DataType.None;
        return new PropertyValue(format(s), false, dataType);
    }

    private PropertyValue arrayValue(Set<String> values) {

        DataType dataType = DataType.None;

        if (inferType){
            for (String value : values) {
                dataType = DataType.getBroadestType(dataType, DataType.identifyType(value, inferDateTimes));
            }
        }

        return new PropertyValue(format(String.join(";", values)), true, dataType);
    }

    private static boolean isArrayCandidate(String s) {
        return s.startsWith("[") && s.endsWith("]");
    }

    private static boolean isArray(JsonNode jsonNode) {
        return jsonNode.isArray();
    }

    private static String format(String s){

        if (s.contains("\"")){
            s = s.replace("\"", "\"\"");
        }

        if (s.contains("\"") || s.contains(",") || s.contains(System.lineSeparator())){
            s = String.format("\"%s\"", s);
        }

        return s;
    }

}
