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

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class PropertySchema {

    private final Object property;
    private boolean isNullable = false;
    private DataType dataType = DataType.None;
    private boolean isMultiValue = false;

    public PropertySchema(Object property) {
        this.property = property;
    }

    public PropertySchema(String property,
                          boolean isNullable,
                          DataType dataType,
                          boolean isMultiValue) {
        this.property = property;
        this.isNullable = isNullable;
        this.dataType = dataType;
        this.isMultiValue = isMultiValue;
    }

    public Object property() {
        return property;
    }

    public int accept(Object value, boolean updateDataType) {

        int size = 1;
        if (isList(value)) {
            List<?> values = (List<?>) value;
            size = values.size();
            if (size != 1) {
                isMultiValue = true;
            }
            if (updateDataType){
                for (Object v : values) {
                    dataType = DataType.getBroadestType(dataType, DataType.dataTypeFor(v.getClass()));
                }
            }
        } else {
            if (updateDataType){
                dataType = DataType.getBroadestType(dataType, DataType.dataTypeFor(value.getClass()));
            }
        }

        return size;
    }

    public void makeNullable(){
        isNullable = true;
    }

    private boolean isList(Object value) {
        return value instanceof List<?>;
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

    public String nameWithDataType(boolean escapeCharacters) {
        return isMultiValue ?
                String.format("%s%s[]", propertyName(property, escapeCharacters), dataType.typeDescription()) :
                String.format("%s%s", propertyName(property, escapeCharacters), dataType.typeDescription());
    }

    public String nameWithoutDataType(boolean escapeCharacters) {
        return propertyName(property, escapeCharacters);
    }

    public String nameWithDataType() {
        return nameWithDataType(false);
    }

    public String nameWithoutDataType() {
        return nameWithoutDataType(false);
    }

    private String propertyName(Object key, boolean escapeCharacters) {
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
        if (escapeCharacters){
            return String.valueOf(key).replace(":", "\\:");
        } else {
            return String.valueOf(key);
        }
    }

    @Override
    public String toString() {
        return "PropertySchema{" +
                "property=" + property +
                ", isNullable=" + isNullable +
                ", dataType=" + dataType +
                ", isMultiValue=" + isMultiValue +
                '}';
    }

    public PropertySchema createCopy() {
        return new PropertySchema(property.toString(), isNullable, dataType, isMultiValue);
    }

    public PropertySchema union(PropertySchema other) {

        if (other.isMultiValue() == isMultiValue &&
                other.dataType() == dataType &&
                other.isNullable() == isNullable) {
            return this;
        }

        boolean newIsNullable = other.isNullable() || isNullable;
        boolean newIsMultiValue = other.isMultiValue() || isMultiValue;
        DataType newDataType = DataType.getBroadestType(dataType, other.dataType());

        return new PropertySchema(
                property.toString(),
                newIsNullable,
                newDataType,
                newIsMultiValue
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertySchema schema = (PropertySchema) o;
        return isNullable == schema.isNullable &&
                isMultiValue == schema.isMultiValue &&
                property.equals(schema.property) &&
                dataType == schema.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(property, isNullable, dataType, isMultiValue);
    }
}
