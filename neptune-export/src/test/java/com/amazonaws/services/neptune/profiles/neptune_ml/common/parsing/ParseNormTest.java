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

package com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing;

import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParseNormTest {

    @Test
    public void throwsErrorIfInvalidNorm() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("norm", "invalid");

        try {
            new ParseNorm(json, new ParsingContext("node feature").withLabel(new Label("Person")).withProperty("age")).parseNorm();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid 'norm' value for node feature (Label: Person, Property: age): 'invalid'. Valid values are: 'none', 'min-max', 'standard'.", e.getMessage());
        }
    }
}