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

import java.util.Collection;
import java.util.Optional;

public class LabelConfig {
    private final String labelType;
    private final String property;
    private final Collection<Double> splitRates;

    public LabelConfig(String labelType, String property, Collection<Double> splitRates) {
        this.labelType = labelType;
        this.property = property;
        this.splitRates = splitRates;

        if (this.splitRates.size() != 3) {
            throw new IllegalArgumentException("split rates must contain 3 values");
        }

        Optional<Double> sum = this.splitRates.stream().reduce(Double::sum);

        if (sum.orElse(0.0) != 1.0) {
            throw new IllegalArgumentException("split rate values must add up to 1.0");
        }
    }

    public String property() {
        return property;
    }

    public Collection<Double> splitRates() {
        return splitRates;
    }

    public String labelType() {
        return labelType;
    }
}
