package com.amazonaws.services.neptune.profiles.neptune_ml.v1.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParseBucketCountV1Test {

    @Test
    public void throwsErrorIfInvalidBucketCount() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("num_buckets", "one");

        try {
            new ParseBucketCountV1(json, new ParsingContext("context")).parseBucketCount();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'num_buckets' field for context. Expected an integer.", e.getMessage());
        }
    }

}