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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.export.FeatureToggle;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;

import java.util.Collection;
import java.util.HashSet;

public class FeatureToggleModule {

    @Option(name = {"--feature-toggle"}, description = "Name of a feature to enable.", hidden = true)
    @AllowedEnumValues(FeatureToggle.class)
    private Collection<FeatureToggle> featureToggles = new HashSet<>();

    public FeatureToggles featureToggles() {
        return new FeatureToggles(featureToggles);
    }
}
