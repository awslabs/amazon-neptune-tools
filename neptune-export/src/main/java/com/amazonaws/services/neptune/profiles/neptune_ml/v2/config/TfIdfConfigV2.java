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

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.amazonaws.services.neptune.propertygraph.Label;

public class TfIdfConfigV2 {

    private final Label label;
    private final String property;
    private final Range ngramRange;
    private final int minDf;
    private final int maxFeatures;

    public TfIdfConfigV2(Label label, String property, Range ngramRange, int minDf, int maxFeatures) {
        this.label = label;
        this.property = property;
        this.ngramRange = ngramRange;
        this.minDf = minDf;
        this.maxFeatures = maxFeatures;
    }

    public Label label() {
        return label;
    }

    public String property() {
        return property;
    }

    public Range ngramRange() {
        return ngramRange;
    }

    public Integer minDf() {
        return minDf;
    }

    public Integer maxFeatures() {
        return maxFeatures;
    }

    @Override
    public String toString() {
        return "TfIdfConfigV2{" +
                "label=" + label +
                ", property='" + property + '\'' +
                ", ngramRange=" + ngramRange +
                ", minDf=" + minDf +
                ", maxFeatures=" + maxFeatures +
                '}';
    }
}
