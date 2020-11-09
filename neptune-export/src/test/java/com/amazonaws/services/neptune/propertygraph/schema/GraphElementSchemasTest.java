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

import static com.amazonaws.services.neptune.util.MapUtils.entry;
import static com.amazonaws.services.neptune.util.MapUtils.map;
import static org.junit.Assert.*;

public class GraphElementSchemasTest {

    @Test
    public void canCreateCopyOfSelf(){

        GraphElementSchemas original = new GraphElementSchemas();

        original.update(new Label("label1"), map(entry("fname", "fname-1")), false);
        original.update(new Label("label1"), map(entry("lname", "lname-1")), false);
        original.update(new Label("label2"), map(entry("fname", "fname-2"), entry("lname", "lname-2")), false);

        GraphElementSchemas copy = original.createCopy();

        assertEquals(original.toJson(), copy.toJson());
        assertNotEquals(original, copy);
    }
}