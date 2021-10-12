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

package com.amazonaws.services.neptune.export;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;

public class ParamConverter {

    private static final String REGEX = "([a-z])([A-Z]+)";
    private static final String REPLACEMENT = "$1-$2";

    public static String toCliArg(String v) {
        return v.replaceAll(REGEX, REPLACEMENT).toLowerCase();
    }

    public static String singularize(String v) {
        if (v.endsWith("ies")) {
            return String.format("%sy", v.substring(0, v.length() - 3));
        }
        else if (v.endsWith("s")) {
            return v.substring(0, v.length() - 1);
        } else {
            return v;
        }
    }

    public static Args fromJson(String cmd, JsonNode json) {
        Args args = new Args(cmd);

        ObjectNode params = (ObjectNode) json;
        Iterator<String> paramNamesIterator = params.fieldNames();
        while (paramNamesIterator.hasNext()) {
            String paramName = paramNamesIterator.next();
            String argName = toCliArg(paramName);
            JsonNode paramNode = params.get(paramName);
            if (paramNode.isArray()) {
                argName = singularize(argName);
                ArrayNode arrayNode = (ArrayNode) paramNode;
                for (JsonNode jsonNode : arrayNode) {
                    addArg(argName, jsonNode, args);
                }
            } else {
                addArg(argName, paramNode, args);
            }
        }

        return args;
    }

    private static void addArg(String argName, JsonNode argValue, Args args) {
        String prefix = argName.startsWith("-") ? "" : "--";
        argName = String.format("%s%s", prefix, argName);
        if (argValue.isBoolean()) {
            if (argValue.asBoolean()){
                args.addFlag(argName);
            }
        } else if (argValue.isObject()) {
            String value = String.format("'%s'", argValue.toPrettyString());
            args.addOption(argName, value);
        } else if (argValue.isTextual()){
            String value = String.format("'%s'", argValue.textValue());
            args.addOption(argName, value);
        }
        else {
            String value = argValue.toString();
            args.addOption(argName, value);
        }
    }

}
