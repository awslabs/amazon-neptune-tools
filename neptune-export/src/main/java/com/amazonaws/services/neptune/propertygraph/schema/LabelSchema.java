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

import com.amazonaws.services.neptune.propertygraph.Label;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class LabelSchema {

    private final Label label;
    private final Map<Object, PropertySchema> propertySchemas = new LinkedHashMap<>();
    private final Map<Object, PropertySchemaStats> propertySchemaStats = new LinkedHashMap<>();

    public LabelSchema(Label label) {
        this.label = label;
    }

    public void put(Object property, PropertySchema propertySchema) {
        put(property, propertySchema, new PropertySchemaStats(property));
    }

    private void put(Object property, PropertySchema propertySchema, PropertySchemaStats stats) {
        if (!property.equals(propertySchema.property())) {
            throw new IllegalStateException(String.format("Property name mismatch: %s, %s", property, propertySchema.property()));
        }
        propertySchemas.put(property, propertySchema);
        propertySchemaStats.put(property, stats);
    }

    public boolean containsProperty(Object property) {
        return propertySchemas.containsKey(property);
    }

    public PropertySchema getPropertySchema(Object property) {
        return propertySchemas.get(property);
    }

    public void recordObservation(PropertySchema propertySchema,
                                  Object value,
                                  PropertySchema.PropertyValueMetadata propertyValueMetadata) {
        if (propertySchema.isNullable()) {
            if (StringUtils.isNotEmpty(String.valueOf(value))) {
                propertySchemaStats.get(propertySchema.property()).recordObservation(propertyValueMetadata);
            }
        } else {
            propertySchemaStats.get(propertySchema.property()).recordObservation(propertyValueMetadata);
        }
    }

    public PropertySchemaStats getPropertySchemaStats(Object property) {
        return propertySchemaStats.get(property);
    }

    public Collection<PropertySchema> propertySchemas() {
        return propertySchemas.values();
    }

    public Collection<PropertySchemaStats> propertySchemaStats() {
        return propertySchemaStats.values();
    }

    public int propertyCount() {
        return propertySchemas.size();
    }

    public Label label() {
        return label;
    }

    public LabelSchema createCopy() {

        LabelSchema result = new LabelSchema(label.createCopy());

        for (PropertySchema schema : propertySchemas.values()) {
            Object property = schema.property();
            result.put(property, schema.createCopy(), propertySchemaStats.get(property).createCopy());
        }

        return result;
    }

    public void initStats() {
        Set<Object> keys = propertySchemaStats.keySet();

        for (Object key : keys) {
            PropertySchemaStats oldStats = this.propertySchemaStats.get(key);
            this.propertySchemaStats.put( key, oldStats.createLockedCopyForFreshObservations());
        }
    }

    public LabelSchema union(LabelSchema other) {

        LabelSchema result = createCopy();

        for (PropertySchema otherSchema : other.propertySchemas()) {
            Object property = otherSchema.property();
            PropertySchemaStats otherSchemaStats = other.getPropertySchemaStats(property);
            if (result.containsProperty(property)) {
                PropertySchema oldSchema = result.getPropertySchema(property);
                PropertySchema newSchema = oldSchema.union(otherSchema);
                PropertySchemaStats oldStats = result.getPropertySchemaStats(property);
                PropertySchemaStats newStats = oldStats.union(otherSchemaStats);
                result.put(property, newSchema, newStats);
            } else {
                result.put(property, otherSchema.createCopy(), otherSchemaStats.createCopy());
            }
        }

        return result;
    }

    public boolean isSameAs(LabelSchema other) {

        if (!label().equals(other.label())) {
            return false;
        }

        if (propertySchemas().size() != other.propertySchemas().size()) {
            return false;
        }

        Iterator<PropertySchema> thisIterator = propertySchemas().iterator();
        Iterator<PropertySchema> otherIterator = other.propertySchemas().iterator();

        while (thisIterator.hasNext()) {
            PropertySchema thisPropertySchema = thisIterator.next();
            PropertySchema otherPropertySchema = otherIterator.next();
            if (!thisPropertySchema.equals(otherPropertySchema)) {
                return false;
            }
        }

        return true;
    }
}
