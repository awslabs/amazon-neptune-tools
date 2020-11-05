package com.amazonaws.services.neptune.dgl.parsing;

import com.amazonaws.services.neptune.dgl.TrainingJobWriterConfig;
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