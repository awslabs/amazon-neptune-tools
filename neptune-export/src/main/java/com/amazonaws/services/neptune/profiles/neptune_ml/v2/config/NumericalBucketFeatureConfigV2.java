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

public class NumericalBucketFeatureConfigV2 {
    private final Label label;
    private final String property;
    private final Range range;
    private final int bucketCount;
    private final int slideWindowSize;
    private final ImputerTypeV2 imputerType;

    public NumericalBucketFeatureConfigV2(Label label,
                                          String property,
                                          Range range,
                                          int bucketCount,
                                          int slideWindowSize,
                                          ImputerTypeV2 imputerType) {

        this.label = label;
        this.property = property;
        this.range = range;
        this.bucketCount = bucketCount;
        this.slideWindowSize = slideWindowSize;
        this.imputerType = imputerType;
    }

    public Label label() {
        return label;
    }

    public String property() {
        return property;
    }

    public Integer bucketCount() {
        return bucketCount;
    }

    public Integer slideWindowSize() {
        return slideWindowSize;
    }

    public Range range() {
        return range;
    }

    public ImputerTypeV2 imputerType() {
        return imputerType;
    }

    @Override
    public String toString() {
        return "NumericalBucketFeatureConfig{" +
                "label=" + label +
                ", property='" + property + '\'' +
                ", range=" + range +
                ", bucketCount=" + bucketCount +
                ", slideWindowSize=" + slideWindowSize +
                ", imputerType=" + imputerType +
                '}';
    }
}
