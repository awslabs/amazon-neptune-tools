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


package com.amazonaws.services.neptune.propertygraph.schema;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class LabelSchema {

    private final String label;
    private final Map<Object, PropertySchema> propertySchemas = new LinkedHashMap<>();

    public LabelSchema(String label) {
        this.label = label;
    }

    public void put(Object property, PropertySchema propertySchema) {
        if (!property.equals(propertySchema.property())) {
            throw new IllegalStateException(String.format("Property name mismatch: %s, %s", property, propertySchema.property()));
        }
        propertySchemas.put(property, propertySchema);
    }

    public boolean containsProperty(Object property) {
        return propertySchemas.containsKey(property);
    }

    public PropertySchema getPropertySchema(Object property) {
        return propertySchemas.get(property);
    }

    public Collection<PropertySchema> propertySchemas() {
        return propertySchemas.values();
    }

    public int propertyCount() {
        return propertySchemas.size();
    }

    public String label() {
        return label;
    }

    public LabelSchema createCopy() {

        LabelSchema result = new LabelSchema(label);

        propertySchemas.values().forEach(p -> result.put(p.property(), p.createCopy()));

        return result;
    }

    public LabelSchema union(LabelSchema other) {

        LabelSchema result = createCopy();

        other.propertySchemas().forEach(p -> {
            Object property = p.property();
            if (result.containsProperty(property)) {
                PropertySchema oldValue = result.getPropertySchema(property);
                PropertySchema newValue = oldValue.createRevision(p);
                result.put(property, newValue);
            } else {
                result.put(property, p.createCopy());
            }
        });

        return result;
    }

    public boolean containsNullableProperties(){
        for (PropertySchema propertySchema : propertySchemas.values()) {
            if (propertySchema.isNullable()){
                return true;
            }
        }
        return false;
    }

    public boolean isSameAs(LabelSchema other){

        if (!label().equals(other.label())){
            return false;
        }

        if (propertySchemas().size() != other.propertySchemas().size()){
            return false;
        }

        Iterator<PropertySchema> thisIterator = propertySchemas().iterator();
        Iterator<PropertySchema> otherIterator = other.propertySchemas().iterator();

        while (thisIterator.hasNext()){
            PropertySchema thisPropertySchema = thisIterator.next();
            PropertySchema otherPropertySchema = otherIterator.next();
            if (!thisPropertySchema.equals(otherPropertySchema)){
                return false;
            }
        }

        return true;
    }
}
