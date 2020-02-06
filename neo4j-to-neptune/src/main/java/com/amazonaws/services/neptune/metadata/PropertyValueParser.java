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

import java.util.HashSet;
import java.util.Set;

public class PropertyValueParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PropertyValueParserPolicy policy;
    private final String semicolonReplacement;
    private final boolean inferType;

    public PropertyValueParser(PropertyValueParserPolicy policy, String semicolonReplacement, boolean inferType) {
        this.policy = policy;
        this.semicolonReplacement = semicolonReplacement;
        this.inferType = inferType;
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
            values.add(format(node.textValue().replace(";", semicolonReplacement)));
        }
        if (values.size() < arrayNode.size()){
            policy.handleDuplicates(s, arrayNode, this);
        }
        return arrayValue(values);
    }

    PropertyValue stringValue(String s){
        DataType dataType = inferType ? DataType.identifyType(s) : DataType.None;
        return new PropertyValue(format(s), false, dataType);
    }

    private PropertyValue arrayValue(Set<String> values) {

        DataType dataType = DataType.None;

        if (inferType){
            for (String value : values) {
                dataType = DataType.getBroadestType(dataType, DataType.identifyType(value));
            }
        }

        return new PropertyValue(String.join(";", values), true, dataType);
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
