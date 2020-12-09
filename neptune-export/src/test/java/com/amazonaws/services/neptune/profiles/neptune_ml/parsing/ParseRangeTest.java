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

package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.TrainingJobWriterConfig;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParseRangeTest {

    @Test
    public void shouldParseRangeFromJson() {

        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.add(1);
        arrayNode.add(10L);
        root.set("range", arrayNode);

        ParseRange parseRange = new ParseRange(root, "Range");
        TrainingJobWriterConfig.Range range = parseRange.parseRange();

        assertEquals(1L, range.low());
        assertEquals(10L, range.high());

    }

    @Test
    public void shouldParseRangeFromJsonWithHighLowSwitched() {

        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.add(10L);
        arrayNode.add(1);
        root.set("range", arrayNode);

        ParseRange parseRange = new ParseRange(root, "Range");
        TrainingJobWriterConfig.Range range = parseRange.parseRange();

        assertEquals(1L, range.low());
        assertEquals(10L, range.high());
    }

}