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

package com.amazonaws.services.neptune.profiles.neptune_ml.v1.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Norm;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Separator;
import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.Collection;

public class FeatureOverrideConfigV1 {
    private final Label label;
    private final Collection<String> properties;
    private final FeatureTypeV1 featureType;
    private final Norm norm;
    private final Separator separator;

    public FeatureOverrideConfigV1(Label label, Collection<String> properties, FeatureTypeV1 featureType, Norm norm, Separator separator) {
        this.label = label;
        this.properties = properties;
        this.featureType = featureType;
        this.norm = norm;
        this.separator = separator;
    }

    public Label label() {
        return label;
    }

    public boolean isSinglePropertyOverride() {
        return properties.size() == 1;
    }

    public String firstProperty() {
        return properties.iterator().next();
    }

    public Collection<String> properties() {
        return properties;
    }

    public FeatureTypeV1 featureType() {
        return featureType;
    }

    public Norm norm() {
        return norm;
    }

    public Separator separator() {
        return separator;
    }
}
