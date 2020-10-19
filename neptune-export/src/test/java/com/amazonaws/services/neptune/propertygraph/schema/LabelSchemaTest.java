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

import static org.junit.Assert.*;

public class LabelSchemaTest {

    @Test
    public void shouldUpdateDataTypesOfExistingProperties(){
        LabelSchema labelSchema1 = new LabelSchema("my-label");

        labelSchema1.put("p1", new PropertySchema("p1", DataType.Integer, false));
        labelSchema1.put("p2", new PropertySchema("p2", DataType.Integer, false));
        labelSchema1.put("p3", new PropertySchema("p3", DataType.Double, false));

        LabelSchema labelSchema2 = new LabelSchema("my-label");

        labelSchema2.put("p1", new PropertySchema("p1", DataType.Double, false));
        labelSchema2.put("p2", new PropertySchema("p2", DataType.Integer, true));
        labelSchema2.put("p3", new PropertySchema("p3", DataType.Integer, false));

        LabelSchema result = labelSchema1.union(labelSchema2);

        assertEquals(result.getPropertySchema("p1"),
                new PropertySchema("p1", DataType.Double, false));
        assertEquals(result.getPropertySchema("p2"),
                new PropertySchema("p2", DataType.Integer, true));
        assertEquals(result.getPropertySchema("p3"),
                new PropertySchema("p3", DataType.Double, false));
    }

    @Test
    public void shouldAddNewPropertiesProperties(){
        LabelSchema labelSchema1 = new LabelSchema("my-label");

        labelSchema1.put("p1", new PropertySchema("p1", DataType.Integer, false));
        labelSchema1.put("p2", new PropertySchema("p2", DataType.Integer, false));
        labelSchema1.put("p3", new PropertySchema("p3", DataType.Double, false));

        LabelSchema labelSchema2 = new LabelSchema("my-label");

        labelSchema2.put("p4", new PropertySchema("p4", DataType.String, false));
        labelSchema2.put("p5", new PropertySchema("p5", DataType.Integer, true));

        LabelSchema result = labelSchema1.union(labelSchema2);

        assertEquals(5, result.properties().size());

        assertEquals(result.getPropertySchema("p4"),
                new PropertySchema("p4", DataType.String, false));
        assertEquals(result.getPropertySchema("p5"),
                new PropertySchema("p5", DataType.Integer, true));
    }
}