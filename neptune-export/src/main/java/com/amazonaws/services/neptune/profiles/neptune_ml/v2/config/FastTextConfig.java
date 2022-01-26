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

import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.Optional;

public class FastTextConfig {
    private final Label label;
    private final String property;
    private final String language;
    private final Integer maxLength;

    public FastTextConfig(Label label, String property, String language, Integer maxLength) {
        this.label = label;
        this.property = property;
        this.language = language;
        this.maxLength = maxLength;
    }

    public Label label() {
        return label;
    }

    public String property() {
        return property;
    }

    public String language() {
        return language;
    }

    public Optional<Integer> maxLength() {
        return Optional.ofNullable(maxLength);
    }

    @Override
    public String toString() {
        return "FastTextConfig{" +
                "label=" + label +
                ", property='" + property + '\'' +
                ", language=" + language +
                ", maxLength=" + maxLength +
                '}';
    }
}
