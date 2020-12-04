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

import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParseEdgeTypeTest {
    @Test
    public void shouldParseEdge() throws JsonProcessingException {
        String json = "{ \"edge\": [\"person\", \"wrote\", \"post\"]}";
        JsonNode jsonNode = new ObjectMapper().readTree(json);
        ParseEdgeType parseEdgeType = new ParseEdgeType(jsonNode, "DESC");
        Label label = parseEdgeType.parseEdgeType();
        assertEquals("person", label.fromLabelsAsString());
        assertEquals("wrote", label.labelsAsString());
        assertEquals("post", label.toLabelsAsString());
    }

    @Test
    public void shouldParseEdgeWithSemicolons() throws JsonProcessingException {
        String json = "{ \"edge\": [\"person;admin\", \"wrote\", \"post;content\"]}";
        JsonNode jsonNode = new ObjectMapper().readTree(json);
        ParseEdgeType parseEdgeType = new ParseEdgeType(jsonNode, "DESC");
        Label label = parseEdgeType.parseEdgeType();
        assertEquals("admin;person", label.fromLabelsAsString());
        assertEquals("wrote", label.labelsAsString());
        assertEquals("content;post", label.toLabelsAsString());
    }

    @Test
    public void shouldEscapeSemicolons() throws JsonProcessingException {
        String json = "{ \"edge\": [\"person;admin\\\\;aa\", \"wrote;x\", \"post;content\"]}";
        JsonNode jsonNode = new ObjectMapper().readTree(json);
        ParseEdgeType parseEdgeType = new ParseEdgeType(jsonNode, "DESC");
        Label label = parseEdgeType.parseEdgeType();
        assertEquals("admin\\;aa;person", label.fromLabelsAsString());
        assertEquals("wrote\\;x", label.labelsAsString());
        assertEquals("content;post", label.toLabelsAsString());
    }
}