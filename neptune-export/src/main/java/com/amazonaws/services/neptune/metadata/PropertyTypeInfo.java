package com.amazonaws.services.neptune.metadata;

import java.util.List;

public class PropertyTypeInfo {

    private final String property;
    private DataType dataType = DataType.None;
    private boolean isMultiValue = false;

    public PropertyTypeInfo(String property) {
        this.property = property;
    }

    public PropertyTypeInfo(String property, DataType dataType, boolean isMultiValue) {
        this.property = property;
        this.dataType = dataType;
        this.isMultiValue = isMultiValue;
    }

    public void accept(Object value) {
        Class cls = Object.class;

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
                String.format("%s%s[]", property, dataType.typeDescription()) :
                String.format("%s%s", property, dataType.typeDescription());
    }

    public String nameWithoutDataType(){
        return property;
    }
}
