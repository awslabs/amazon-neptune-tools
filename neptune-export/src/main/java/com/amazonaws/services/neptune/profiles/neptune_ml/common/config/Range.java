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

import com.amazonaws.services.neptune.propertygraph.schema.DataType;

public class Range {
    private final Object low;
    private final Object high;

    public Range(Object low, Object high) {

        DataType lowDataType = DataType.dataTypeFor(low.getClass());
        DataType highDataType = DataType.dataTypeFor(high.getClass());

        if (!lowDataType.isNumeric() || !highDataType.isNumeric()) {
            throw new IllegalArgumentException("Low and high values must be numeric");
        }

        DataType dataType = DataType.getBroadestType(lowDataType, highDataType);

        Object highValue = dataType.convert(high);
        Object lowValue = dataType.convert(low);

        this.high = dataType.compare(highValue, lowValue) >= 0 ? highValue : lowValue;
        this.low = dataType.compare(highValue, lowValue) >= 0 ? lowValue : highValue;
    }

    public Object low() {
        return low;
    }

    public Object high() {
        return high;
    }

    @Override
    public String toString() {
        return "Range{" +
                "low=" + low +
                ", high=" + high +
                '}';
    }
}
