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
import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.*;

public class LabelSchemaTest {

    @Test
    public void unioningShouldUpdateDataTypesOfExistingProperties(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p3", new PropertySchema("p3", false, DataType.Double, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Double, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p2", new PropertySchema("p2", false, DataType.Integer, true, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p3", new PropertySchema("p3", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema result = labelSchema1.union(labelSchema2);

        assertEquals(result.getPropertySchema("p1"),
                new PropertySchema("p1", false, DataType.Double, false, EnumSet.noneOf(DataType.class)));
        assertEquals(result.getPropertySchema("p2"),
                new PropertySchema("p2", false, DataType.Integer, true, EnumSet.noneOf(DataType.class)));
        assertEquals(result.getPropertySchema("p3"),
                new PropertySchema("p3", false, DataType.Double, false, EnumSet.noneOf(DataType.class)));
    }

    @Test
    public void unioningShouldAddNewProperties(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p3", new PropertySchema("p3", false, DataType.Double, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p4", new PropertySchema("p4", false, DataType.String, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p5", new PropertySchema("p5", false, DataType.Integer, true, EnumSet.noneOf(DataType.class)));

        LabelSchema result = labelSchema1.union(labelSchema2);

        assertEquals(5, result.propertySchemas().size());

        assertEquals(result.getPropertySchema("p4"),
                new PropertySchema("p4", false, DataType.String, false, EnumSet.noneOf(DataType.class)));
        assertEquals(result.getPropertySchema("p5"),
                new PropertySchema("p5", false, DataType.Integer, true, EnumSet.noneOf(DataType.class)));
    }

    @Test
    public void schemasWithSameLabelAndPropertySchemasAreSame(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        assertTrue(labelSchema1.isSameAs(labelSchema2));
    }

    @Test
    public void schemasWithDifferentLabelsAreNotSame(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("this-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("that-label"));

        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        assertFalse(labelSchema1.isSameAs(labelSchema2));
    }

    @Test
    public void schemasWithDifferentPropertiesAreNotSame(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Double, true, EnumSet.noneOf(DataType.class)));

        assertFalse(labelSchema1.isSameAs(labelSchema2));
    }

    @Test
    public void schemasWithDifferentNumberOfPropertiesAreNotSame(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p3", new PropertySchema("p3", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema3 = new LabelSchema(new Label("my-label"));

        labelSchema3.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        assertFalse(labelSchema1.isSameAs(labelSchema2));
        assertFalse(labelSchema1.isSameAs(labelSchema3));
    }

    @Test
    public void schemasWithPropertySchemasInDifferentOrderAreNotSame(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        assertFalse(labelSchema1.isSameAs(labelSchema2));
    }

    @Test
    public void schemasWithPropertiesWithDifferentNullableCharacteristicsAreNotSame(){
        LabelSchema labelSchema1 = new LabelSchema(new Label("my-label"));

        labelSchema1.put("p1", new PropertySchema("p1", true, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema1.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        LabelSchema labelSchema2 = new LabelSchema(new Label("my-label"));

        labelSchema2.put("p1", new PropertySchema("p1", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));
        labelSchema2.put("p2", new PropertySchema("p2", false, DataType.Integer, false, EnumSet.noneOf(DataType.class)));

        assertFalse(labelSchema1.isSameAs(labelSchema2));
    }
}