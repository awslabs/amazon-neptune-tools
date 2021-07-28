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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ErrorMessageHelper {

    public static IllegalArgumentException invalidFieldValue(String fieldName, String value, ParsingContext context, Collection<String> validValues) {
        return new IllegalArgumentException(String.format("Invalid '%s' value for %s: '%s'. Valid values are: %s.",
                fieldName,
                context,
                value,
                ErrorMessageHelper.quoteList(validValues)));
    }

    public static IllegalArgumentException errorParsingField(String fieldName, ParsingContext context, String expected) {
        return new IllegalArgumentException(String.format("Error parsing '%s' field for %s. Expected %s.",
                fieldName,
                context,
                expected));
    }

    public static String quoteList(Collection<String> values) {
        return values.stream().map(s -> String.format("'%s'", s)).collect(Collectors.joining(", "));
    }

    public static String quoteList(List<Enum<?>> enums) {
        return quoteList(enumNames(enums));
    }

    public static Collection<String> enumNames(List<Enum<?>> enums) {
        return enums.stream().map(Enum::name).collect(Collectors.toList());
    }
}
