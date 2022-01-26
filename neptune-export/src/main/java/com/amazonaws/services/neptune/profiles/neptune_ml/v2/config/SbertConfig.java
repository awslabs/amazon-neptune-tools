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

public class SbertConfig {

    private final Label label;
    private final String property;
    private final String name;

    public SbertConfig(Label label, String property, String name) {
        this.label = label;
        this.property = property;
        this.name = name;
    }

    public Label label() {
        return label;
    }

    public String property() {
        return property;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "SbertConfig{" +
                "label=" + label +
                ", property='" + property + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
