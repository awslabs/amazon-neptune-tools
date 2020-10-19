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

        metadata1.union(metadata2);

        assertEquals(metadata1.getPropertyTypeInfo("p1"),
                new PropertyTypeInfo("p1", DataType.Double, false));
        assertEquals(metadata1.getPropertyTypeInfo("p2"),
                new PropertyTypeInfo("p2", DataType.Integer, true));
        assertEquals(metadata1.getPropertyTypeInfo("p3"),
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

        assertEquals(3, metadata1.properties().size());

        metadata1.union(metadata2);

        assertEquals(5, metadata1.properties().size());

        assertEquals(metadata1.getPropertyTypeInfo("p4"),
                new PropertyTypeInfo("p4", DataType.String, false));
        assertEquals(metadata1.getPropertyTypeInfo("p5"),
                new PropertyTypeInfo("p5", DataType.Integer, true));
    }


}