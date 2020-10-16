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

    private final Map<Object, PropertyTypeInfo> propertyMetadata = new LinkedHashMap<>();
    private final Set<String> outputIds = new HashSet<>();

    public void addOutputId(String outputId) {
        this.outputIds.add(outputId);
    }

    public void put(Object property, PropertyTypeInfo propertyTypeInfo) {
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
}
