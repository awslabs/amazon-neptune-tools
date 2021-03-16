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

import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.Collection;

public class Word2VecConfig {
    private final Label label;
    private final String property;
    private final Collection<String> languages;

    public Word2VecConfig(Label label, String property, Collection<String> languages) {
        this.label = label;
        this.property = property;
        this.languages = languages;
    }

    public Label label() {
        return label;
    }

    public String property() {
        return property;
    }

    public Collection<String> languages() {
        return languages;
    }

    @Override
    public String toString() {
        return "Word2VecConfig{" +
                "label=" + label +
                ", property='" + property + '\'' +
                ", languages=" + languages +
                '}';
    }
}
