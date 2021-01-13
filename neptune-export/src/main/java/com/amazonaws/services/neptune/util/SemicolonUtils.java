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

package com.amazonaws.services.neptune.util;

import java.util.Arrays;
import java.util.Collection;

public class SemicolonUtils {

    private static final String SEMICOLON_SEPARATOR = "(?<!\\\\);";

    public static Collection<String> split(String s){
        return Arrays.asList(s.split(SEMICOLON_SEPARATOR));
    }

    public static String unescapeSingleValue(String s){
        Collection<String> values = split(s);
        if (values.size() == 1){
           return values.iterator().next().replace("\\;", ";");
        } else {
            return s;
        }
    }

}
