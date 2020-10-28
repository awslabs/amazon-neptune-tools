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

import java.util.List;
import java.util.Objects;

public class PropertySchema {

    private final Object property;
    private boolean isNullable = false;
    private DataType dataType = DataType.None;
    private boolean isMultiValue = false;
    private int minMultiValueSize = 0;
    private int maxMultiValueSize = 0;

    public PropertySchema(Object property) {
        this.property = property;
    }

    public PropertySchema(String property,
                          boolean isNullable,
                          DataType dataType,
                          boolean isMultiValue,
                          int minMultiValueSize,
                          int maxMultiValueSize) {
        this.property = property;
        this.isNullable = isNullable;
        this.dataType = dataType;
        this.isMultiValue = isMultiValue;
        this.minMultiValueSize = minMultiValueSize;
        this.maxMultiValueSize = maxMultiValueSize;
    }

    public Object property() {
        return property;
    }

    public void accept(Object value) {

        if (isList(value)) {
            List<?> values = (List<?>) value;
            int size = values.size();
            if (size > maxMultiValueSize){
                maxMultiValueSize = size;
            }
            if (minMultiValueSize > 0 && minMultiValueSize > size){
                minMultiValueSize = size;
            }
            if (size > 1) {
                isMultiValue = true;
            }
            for (Object v : values) {
                dataType = DataType.getBroadestType(dataType, DataType.dataTypeFor(v.getClass()));
            }
        } else {
            dataType = DataType.getBroadestType(dataType, DataType.dataTypeFor(value.getClass()));
        }
    }

    public void makeNullable(){
        isNullable = true;
    }

    private boolean isList(Object value) {
        return value.getClass().isAssignableFrom(java.util.ArrayList.class);
    }

    public DataType dataType() {
        return dataType;
    }

    public boolean isMultiValue() {
        return isMultiValue;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public boolean isUniformMultiValueSize(){
        return minMultiValueSize == maxMultiValueSize;
    }

    public int minMultiValueSize() {
        return minMultiValueSize;
    }

    public int maxMultiValueSize() {
        return maxMultiValueSize;
    }

    public String nameWithDataType() {
        return isMultiValue ?
                String.format("%s%s[]", propertyName(property), dataType.typeDescription()) :
                String.format("%s%s", propertyName(property), dataType.typeDescription());
    }

    public String nameWithoutDataType() {
        return propertyName(property);
    }

    private String propertyName(Object key) {
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.label)) {
            return "~label";
        }
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.id)) {
            return "~id";
        }
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.key)) {
            return "~key";
        }
        if (key.equals(org.apache.tinkerpop.gremlin.structure.T.value)) {
            return "~value";
        }
        return String.valueOf(key);
    }

    @Override
    public String toString() {
        return "PropertySchema{" +
                "property=" + property +
                ", isNullable=" + isNullable +
                ", dataType=" + dataType +
                ", isMultiValue=" + isMultiValue +
                ", minMultiValueSize=" + minMultiValueSize +
                ", maxMultiValueSize=" + maxMultiValueSize +
                '}';
    }

    public PropertySchema createCopy() {
        return new PropertySchema(property.toString(), isNullable, dataType, isMultiValue, minMultiValueSize, maxMultiValueSize);
    }

    public PropertySchema createRevision(PropertySchema propertySchema) {

        if (propertySchema.isMultiValue() == isMultiValue &&
                propertySchema.dataType() == dataType &&
                propertySchema.isNullable() == isNullable) {
            return this;
        }

        boolean newIsNullable = propertySchema.isNullable() || isNullable;
        boolean newIsMultiValue = propertySchema.isMultiValue() || isMultiValue;
        DataType newDataType = DataType.getBroadestType(dataType, propertySchema.dataType());
        int newMinMultiValueSize = Math.min(minMultiValueSize, propertySchema.minMultiValueSize());
        int newMaxMultiValueSize = Math.max(maxMultiValueSize, propertySchema.maxMultiValueSize());

        return new PropertySchema(property.toString(), newIsNullable, newDataType, newIsMultiValue, newMinMultiValueSize, newMaxMultiValueSize );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertySchema schema = (PropertySchema) o;
        return isNullable == schema.isNullable &&
                isMultiValue == schema.isMultiValue &&
                minMultiValueSize == schema.minMultiValueSize &&
                maxMultiValueSize == schema.maxMultiValueSize &&
                property.equals(schema.property) &&
                dataType == schema.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(property, isNullable, dataType, isMultiValue, minMultiValueSize, maxMultiValueSize);
    }
}
