package com.amazonaws.services.neptune.propertygraph.schema;

import org.junit.Test;

import static org.junit.Assert.*;

public class PropertySchemaTest {

    @Test
    public void revisionWhereAtLeastOneSchemaIsMultiValueShouldResultInMultiValue(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, false);
        PropertySchema schema2 = new PropertySchema("p1", false, DataType.Integer, true);

        assertTrue(schema1.union(schema2).isMultiValue());
        assertTrue(schema2.union(schema1).isMultiValue());
    }

    @Test
    public void revisionWhereAtLeastOneSchemaIsNullableShouldResultInNullable(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, false);
        PropertySchema schema2 = new PropertySchema("p1", true, DataType.Integer, false);

        assertTrue(schema1.union(schema2).isNullable());
        assertTrue(schema2.union(schema1).isNullable());
    }
}