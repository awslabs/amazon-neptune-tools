/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph.schema;

public class PropertySchemaStats {

    private final Object property;
    private int minMultiValueSize = 0;
    private int maxMultiValueSize = 0;
    private int observationCount = 0;

    public PropertySchemaStats(Object property) {
        this.property = property;
    }

    public PropertySchemaStats(Object property, int minMultiValueSize, int maxMultiValueSize, int observationCount) {
        this.property = property;
        this.minMultiValueSize = minMultiValueSize;
        this.maxMultiValueSize = maxMultiValueSize;
        this.observationCount = observationCount;
    }

    public void updateMultiValueSize(int size) {
        if (size > maxMultiValueSize) {
            maxMultiValueSize = size;
        }
        if (minMultiValueSize > 0 && minMultiValueSize > size) {
            minMultiValueSize = size;
        }
    }

    public void recordObservation() {
        observationCount++;
    }

    public Object property() {
        return property;
    }

    public int observationCount() {
        return observationCount;
    }

    public int minMultiValueSize() {
        return minMultiValueSize;
    }

    public int maxMultiValueSize() {
        return maxMultiValueSize;
    }

    public PropertySchemaStats union(PropertySchemaStats other) {
        int newMinMultiValueSize = Math.min(minMultiValueSize, other.minMultiValueSize());
        int newMaxMultiValueSize = Math.max(maxMultiValueSize, other.maxMultiValueSize());
        int newObservationCount = observationCount + other.observationCount();
        return new PropertySchemaStats(property, newMinMultiValueSize, newMaxMultiValueSize, newObservationCount);
    }

    public PropertySchemaStats createCopy() {
        return new PropertySchemaStats(property, minMultiValueSize, maxMultiValueSize, observationCount);
    }

    @Override
    public String toString() {
        return "PropertySchemaStats{" +
                "property=" + property +
                ", minMultiValueSize=" + minMultiValueSize +
                ", maxMultiValueSize=" + maxMultiValueSize +
                ", observationCount=" + observationCount +
                '}';
    }

}
