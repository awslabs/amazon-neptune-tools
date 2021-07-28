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

package com.amazonaws.services.neptune.profiles.neptune_ml.common.config;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

public class Separator {
    private static final String DEFAULT_SEPARATOR = ";";

    private final String separator;

    public Separator() {
        this(null);
    }

    public Separator(String separator) {
        this.separator = separator;
    }

    public void writeTo(JsonGenerator generator, boolean isMultiValue) throws IOException {
        if (StringUtils.isNotEmpty(separator)) {
            generator.writeStringField("separator", separator);
        } else if (isMultiValue) {
            generator.writeStringField("separator", DEFAULT_SEPARATOR);
        }
    }
}
