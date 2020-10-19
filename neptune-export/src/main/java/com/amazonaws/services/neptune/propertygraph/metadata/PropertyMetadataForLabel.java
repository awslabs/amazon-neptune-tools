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


package com.amazonaws.services.neptune.propertygraph.metadata;

import java.util.*;

public class PropertyMetadataForLabel {

    private final String label;
    private final Map<Object, PropertyTypeInfo> propertyMetadata = new LinkedHashMap<>();
    private final Set<String> outputIds = new HashSet<>();

    public PropertyMetadataForLabel(String label) {
        this.label = label;
    }

    public void addOutputId(String outputId) {
        this.outputIds.add(outputId);
    }

    public void put(Object property, PropertyTypeInfo propertyTypeInfo) {
        if (!property.equals(propertyTypeInfo.property())){
            throw new IllegalStateException(String.format("Property name mismatch: %s, %s", property, propertyTypeInfo.property()));
        }
        propertyMetadata.put(property, propertyTypeInfo);
    }

    public boolean containsProperty(Object property) {
        return propertyMetadata.containsKey(property);
    }

    public PropertyTypeInfo getPropertyTypeInfo(Object property) {
        return propertyMetadata.get(property);
    }

    public Collection<PropertyTypeInfo> properties() {
        return propertyMetadata.values();
    }

    public int propertyCount() {
        return propertyMetadata.size();
    }

    public String label() {
        return label;
    }

    public Collection<String> outputIds() {
        return outputIds;
    }

    public void union(PropertyMetadataForLabel propertyMetadataForLabel) {
        for (PropertyTypeInfo propertyTypeInfo : propertyMetadataForLabel.properties()) {
            Object property = propertyTypeInfo.property();
            if (propertyMetadata.containsKey(property)) {
                PropertyTypeInfo oldValue = propertyMetadata.get(property);
                PropertyTypeInfo newValue = oldValue.createRevision(propertyTypeInfo);
                propertyMetadata.put(property, newValue);
            } else {
                propertyMetadata.put(property, propertyTypeInfo.createCopy());
            }
        }
    }
}
