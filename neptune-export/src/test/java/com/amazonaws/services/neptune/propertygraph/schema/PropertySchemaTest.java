package com.amazonaws.services.neptune.propertygraph.schema;

import org.junit.Test;

import static org.junit.Assert.*;

public class PropertySchemaTest {

    @Test
    public void revisionWhereAtLeastOneSchemaIsMultiValueShouldResultInMultiValue(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, false, 0, 0);
        PropertySchema schema2 = new PropertySchema("p1", false, DataType.Integer, true, 0, 0);

        assertTrue(schema1.createRevision(schema2).isMultiValue());
        assertTrue(schema2.createRevision(schema1).isMultiValue());
    }

    @Test
    public void revisionWhereAtLeastOneSchemaIsNullableShouldResultInNullable(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, false, 0, 0);
        PropertySchema schema2 = new PropertySchema("p1", true, DataType.Integer, false, 0, 0);

        assertTrue(schema1.createRevision(schema2).isNullable());
        assertTrue(schema2.createRevision(schema1).isNullable());
    }

    @Test
    public void revisionWithDifferingMultiValueSizesShouldResultInMinAndMax(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, true, 2, 6);
        PropertySchema schema2 = new PropertySchema("p1", true, DataType.Integer, true, 1, 5);

        assertEquals(1, schema1.createRevision(schema2).minMultiValueSize());
        assertEquals(6, schema1.createRevision(schema2).maxMultiValueSize());
        assertEquals(1, schema2.createRevision(schema1).minMultiValueSize());
        assertEquals(6, schema2.createRevision(schema1).maxMultiValueSize());
    }


}