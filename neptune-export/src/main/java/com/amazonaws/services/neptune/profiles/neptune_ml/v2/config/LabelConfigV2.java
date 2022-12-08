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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.Collection;

public class LabelConfigV2 {

    private final Label nodeType;
    private final String taskType;
    private final String property;
    private final String subject;
    private final String object;
    private final Collection<Double> splitRates;
    private final Separator separator;

    public LabelConfigV2(Label nodeType,
                         String taskType,
                         String property,
                         String subject,
                         String object,
                         Collection<Double> splitRates,
                         Separator separator) {
        this.nodeType = nodeType;
        this.taskType = taskType;
        this.property = property;
        this.subject = subject;
        this.object = object;
        this.splitRates = splitRates;
        this.separator = separator;
    }

    public Label label() {
        return nodeType;
    }

    public String taskType() {
        return taskType;
    }

    public String property() {
        return property;
    }

    public String subject() {
        return subject;
    }

    public String object() {
        return object;
    }

    public Collection<Double> splitRates() {
        return splitRates;
    }

    public Separator separator() {
        return separator;
    }
}
