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
    private final boolean lockMultiValueSizes;
    private int minMultiValueSize;
    private int maxMultiValueSize;
    private int observationCount;

    public PropertySchemaStats(Object property) {
        this(property, -1, -1, 0, false);
    }

    public PropertySchemaStats(Object property, int minMultiValueSize, int maxMultiValueSize, int observationCount, boolean lockMultiValueSizes) {
        this.property = property;
        this.minMultiValueSize = minMultiValueSize;
        this.maxMultiValueSize = maxMultiValueSize;
        this.observationCount = observationCount;
        this.lockMultiValueSizes = lockMultiValueSizes;
    }

    public void recordObservation(int size) {
        observationCount++;
        if (!lockMultiValueSizes) {
            if (minMultiValueSize < 0) {
                minMultiValueSize = size;
                maxMultiValueSize = size;
            }
            if (size > maxMultiValueSize) {
                maxMultiValueSize = size;
            }
            if (size < minMultiValueSize) {
                minMultiValueSize = size;
            }
        }
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

    public boolean isUniformMultiValueSize(){
        return minMultiValueSize == maxMultiValueSize;
    }

    public PropertySchemaStats union(PropertySchemaStats other) {
        int newMinMultiValueSize = Math.min(minMultiValueSize, other.minMultiValueSize());
        int newMaxMultiValueSize = Math.max(maxMultiValueSize, other.maxMultiValueSize());
        int newObservationCount = observationCount + other.observationCount();
        return new PropertySchemaStats(property, newMinMultiValueSize, newMaxMultiValueSize, newObservationCount, false);
    }

    public PropertySchemaStats createCopy() {
        return new PropertySchemaStats(property, minMultiValueSize, maxMultiValueSize, observationCount, false);
    }

    public PropertySchemaStats createLockedCopyForFreshObservations(){
        return new PropertySchemaStats(
                property,
                minMultiValueSize,
                maxMultiValueSize,
                0,
                true);
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
