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

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.*;

public class PropertySchemaTest {

    @Test
    public void revisionWhereAtLeastOneSchemaIsMultiValueShouldResultInMultiValue(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class));
        PropertySchema schema2 = new PropertySchema("p1", false, DataType.Integer, true, EnumSet.noneOf(DataType.class));

        assertTrue(schema1.union(schema2).isMultiValue());
        assertTrue(schema2.union(schema1).isMultiValue());
    }

    @Test
    public void revisionWhereAtLeastOneSchemaIsNullableShouldResultInNullable(){
        PropertySchema schema1 = new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class));
        PropertySchema schema2 = new PropertySchema("p1", true, DataType.Integer, false, EnumSet.noneOf(DataType.class));

        assertTrue(schema1.union(schema2).isNullable());
        assertTrue(schema2.union(schema1).isNullable());
    }

    @Test
    public void shouldEscapePropertyNameContainingColons(){
        PropertySchema schema = new PropertySchema("p1:a:b:c", false, DataType.Integer, false, EnumSet.noneOf(DataType.class));
        assertEquals("p1\\:a\\:b\\:c:int", schema.nameWithDataType(true));
        assertEquals("p1\\:a\\:b\\:c", schema.nameWithoutDataType(true));

        assertEquals("p1:a:b:c:int", schema.nameWithDataType());
        assertEquals("p1:a:b:c", schema.nameWithoutDataType());
    }
}