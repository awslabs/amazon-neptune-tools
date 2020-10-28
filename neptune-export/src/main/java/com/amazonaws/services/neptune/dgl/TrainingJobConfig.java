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

package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.*;

public class TrainingJobConfig {

    private final Map<Label, String> nodeClassLabels;
    private final Collection<Double> splitRates;

    public TrainingJobConfig() {
        this(Collections.emptyMap(), Arrays.asList(0.7, 0.1, 0.2));
    }

    public TrainingJobConfig(Map<Label, String> nodeClassLabels, Collection<Double> splitRates) {
        this.nodeClassLabels = nodeClassLabels;
        this.splitRates = splitRates;

        if (this.splitRates.size() != 3) {
            throw new IllegalArgumentException("splitRates must contain 3 values");
        }

        Optional<Double> sum = this.splitRates.stream().reduce(Double::sum);

        if (sum.orElse(0.0) != 1.0) {
            throw new IllegalArgumentException("splitRates values must add up to 1.0");
        }
    }

    public boolean hasNodeClassSpecificationForNodeType(Label label) {
        return nodeClassLabels.containsKey(label);
    }

    public String getColumnForNodeClass(Label label) {
        return nodeClassLabels.get(label);
    }

    public Collection<Double> splitRates() {
        return splitRates;
    }
}
