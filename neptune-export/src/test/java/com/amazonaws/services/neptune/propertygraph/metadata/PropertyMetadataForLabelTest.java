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

import org.junit.Test;

import static org.junit.Assert.*;

public class PropertyMetadataForLabelTest {

    @Test
    public void shouldUpdateDataTypesOfExistingProperties(){
        PropertyMetadataForLabel metadata1 = new PropertyMetadataForLabel("my-label");

        metadata1.put("p1", new PropertyTypeInfo("p1", DataType.Integer, false));
        metadata1.put("p2", new PropertyTypeInfo("p2", DataType.Integer, false));
        metadata1.put("p3", new PropertyTypeInfo("p3", DataType.Double, false));

        PropertyMetadataForLabel metadata2 = new PropertyMetadataForLabel("my-label");

        metadata2.put("p1", new PropertyTypeInfo("p1", DataType.Double, false));
        metadata2.put("p2", new PropertyTypeInfo("p2", DataType.Integer, true));
        metadata2.put("p3", new PropertyTypeInfo("p3", DataType.Integer, false));

        PropertyMetadataForLabel result = metadata1.union(metadata2);

        assertEquals(result.getPropertyTypeInfo("p1"),
                new PropertyTypeInfo("p1", DataType.Double, false));
        assertEquals(result.getPropertyTypeInfo("p2"),
                new PropertyTypeInfo("p2", DataType.Integer, true));
        assertEquals(result.getPropertyTypeInfo("p3"),
                new PropertyTypeInfo("p3", DataType.Double, false));
    }

    @Test
    public void shouldAddNewPropertiesProperties(){
        PropertyMetadataForLabel metadata1 = new PropertyMetadataForLabel("my-label");

        metadata1.put("p1", new PropertyTypeInfo("p1", DataType.Integer, false));
        metadata1.put("p2", new PropertyTypeInfo("p2", DataType.Integer, false));
        metadata1.put("p3", new PropertyTypeInfo("p3", DataType.Double, false));

        PropertyMetadataForLabel metadata2 = new PropertyMetadataForLabel("my-label");

        metadata2.put("p4", new PropertyTypeInfo("p4", DataType.String, false));
        metadata2.put("p5", new PropertyTypeInfo("p5", DataType.Integer, true));

        PropertyMetadataForLabel result = metadata1.union(metadata2);

        assertEquals(5, result.properties().size());

        assertEquals(result.getPropertyTypeInfo("p4"),
                new PropertyTypeInfo("p4", DataType.String, false));
        assertEquals(result.getPropertyTypeInfo("p5"),
                new PropertyTypeInfo("p5", DataType.Integer, true));
    }
}