package com.amazonaws.services.neptune.propertygraph.metadata;

import org.junit.Test;

import static com.amazonaws.services.neptune.util.MapUtils.entry;
import static com.amazonaws.services.neptune.util.MapUtils.map;
import static org.junit.Assert.*;

public class PropertyMetadataForLabelsTest {

    @Test
    public void canCreateCopyOfSelf(){

        PropertyMetadataForLabels original = new PropertyMetadataForLabels();

        original.update("label1", map(entry("fname", "fname-1")), false);
        original.update("label1", map(entry("lname", "lname-1")), false);
        original.update("label2", map(entry("fname", "fname-2"), entry("lname", "lname-2")), false);

        PropertyMetadataForLabels copy = original.createCopy();

        assertEquals(original.toJson(), copy.toJson());
        assertNotEquals(original, copy);
    }
}