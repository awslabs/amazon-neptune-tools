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

package com.amazonaws.services.neptune.propertygraph.metadata;

import java.util.List;

public class PropertyTypeInfo {

    private final Object property;
    private DataType dataType = DataType.None;
    private boolean isMultiValue = false;

    public PropertyTypeInfo(Object property) {
        this.property = property;
    }

    public PropertyTypeInfo(String property, DataType dataType, boolean isMultiValue) {
        this.property = property;
        this.dataType = dataType;
        this.isMultiValue = isMultiValue;
    }

    public void accept(Object value) {
        Class<?> cls = Object.class;

        if (isList(value)) {
            List<?> values = (List<?>) value;
            if (values.size() > 1) {
                isMultiValue = true;
            }
            if (!values.isEmpty()) {
                cls = values.get(0).getClass();
            }
        } else {
            cls = value.getClass();
        }

        dataType = DataType.getBroadestType(dataType, DataType.dataTypeFor(cls));
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

    public String nameWithDataType(){
        return isMultiValue ?
                String.format("%s%s[]", propertyName(property), dataType.typeDescription()) :
                String.format("%s%s", propertyName(property), dataType.typeDescription());
    }

    public String nameWithoutDataType(){
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
}
