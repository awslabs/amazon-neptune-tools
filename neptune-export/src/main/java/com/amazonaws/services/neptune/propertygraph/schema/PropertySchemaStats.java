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

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PropertySchemaStats {

    private final Object property;
    private final boolean lock;
    private int minCardinality;
    private int maxCardinality;
    private long observationCount;
    private long numberValuesCount;
    private final EnumMap<DataType, Integer> dataTypeCounts;

    public PropertySchemaStats(Object property) {
        this(property, -1, -1, 0, 0, new EnumMap<>(DataType.class), false);
    }

    public PropertySchemaStats(Object property,
                               int minCardinality,
                               int maxCardinality,
                               long observationCount,
                               long numberValuesCount,
                               EnumMap<DataType, Integer> dataTypeCounts,
                               boolean lock) {
        this.property = property;
        this.minCardinality = minCardinality;
        this.maxCardinality = maxCardinality;
        this.observationCount = observationCount;
        this.numberValuesCount = numberValuesCount;
        this.dataTypeCounts = dataTypeCounts;
        this.lock = lock;
    }

    public void recordObservation(PropertySchema.PropertyValueMetadata propertyValueMetadata) {
        observationCount++;
        if (!lock) {
            int size = propertyValueMetadata.size();
            if (minCardinality < 0) {
                minCardinality = size;
                maxCardinality = size;
            }
            if (size > maxCardinality) {
                maxCardinality = size;
            }
            if (size < minCardinality) {
                minCardinality = size;
            }
            numberValuesCount += size;
            propertyValueMetadata.addTo(dataTypeCounts);
        }
    }

    public Object property() {
        return property;
    }

    public long observationCount() {
        return observationCount;
    }

    public long numberValuesCount() {
        return numberValuesCount;
    }

    public int minCardinality() {
        return minCardinality;
    }

    public int maxCardinality() {
        return maxCardinality;
    }

    public boolean isUniformCardinality() {
        return minCardinality == maxCardinality;
    }

    public EnumMap<DataType, Integer> dataTypeCounts() {
        return dataTypeCounts;
    }

    public PropertySchemaStats union(PropertySchemaStats other) {
        int newMinCardinality = Math.min(minCardinality, other.minCardinality());
        int newMaxCardinality = Math.max(maxCardinality, other.maxCardinality());
        long newObservationCount = observationCount + other.observationCount();
        long newNumberValuesCount = numberValuesCount + other.numberValuesCount();
        EnumMap<DataType, Integer> newDataTypeCounts = new EnumMap<DataType, Integer>(DataType.class);
        newDataTypeCounts.putAll(dataTypeCounts);
        for (Map.Entry<DataType, Integer> entry : other.dataTypeCounts.entrySet()) {
            DataType key = entry.getKey();
            int i = newDataTypeCounts.containsKey(key) ? newDataTypeCounts.get(key) : 0;
            newDataTypeCounts.put(key, i + entry.getValue());
        }
        return new PropertySchemaStats(property, newMinCardinality, newMaxCardinality, newObservationCount, newNumberValuesCount, newDataTypeCounts, false);
    }

    public PropertySchemaStats createCopy() {
        EnumMap<DataType, Integer> newDataTypeCounts = new EnumMap<DataType, Integer>(DataType.class);
        newDataTypeCounts.putAll(dataTypeCounts);
        return new PropertySchemaStats(property, minCardinality, maxCardinality, observationCount, numberValuesCount, newDataTypeCounts, false);
    }

    public PropertySchemaStats createLockedCopyForFreshObservations() {
        return new PropertySchemaStats(
                property,
                minCardinality,
                maxCardinality,
                0,
                numberValuesCount,
                dataTypeCounts, true
        );
    }

    @Override
    public String toString() {
        String s = dataTypeCounts.entrySet().stream().
                map(e -> e.getKey().name() + ":" + e.getValue()).
                collect(Collectors.joining(","));


        return property + " {" +
                "propertyCount=" + observationCount +
                ", minCardinality=" + minCardinality +
                ", maxCardinality=" + maxCardinality +
                ", recordCount=" + numberValuesCount +
                ", dataTypeCounts=[" + s + "]" +
                "}";
    }

}
