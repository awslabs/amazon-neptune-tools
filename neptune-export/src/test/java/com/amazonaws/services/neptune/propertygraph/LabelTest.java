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

package com.amazonaws.services.neptune.propertygraph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class LabelTest {

    @Test
    public void shouldParseSimpleNodeLabelFromJson() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : \"label1\"\n" +
                "}";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("label1", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseComplexNodeLabelFromSingleSemiColonSeparatedStringValue() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : \"labelB;labelA\"\n" +
                "}";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("labelA;labelB", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseComplexNodeLabelFromArray() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : [ \"labelB\", \"labelA\" ]\n" +
                "}";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("labelA;labelB", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseEdgeLabelFromJson() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\",\n" +
                "      \"~fromLabels\" : [ \"startLabel\" ],\n" +
                "      \"~toLabels\" : [ \"endLabel\" ]\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("(startLabel)-edgeLabel-(endLabel)", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseEdgeLabelFromJsonWithSimpleStringStartAndEndVertexLabels() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\",\n" +
                "      \"~fromLabels\" : \"startLabel\",\n" +
                "      \"~toLabels\" : \"endLabel\"\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("(startLabel)-edgeLabel-(endLabel)", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseEdgeLabelFromJsonWithMissingStartVertexLabel() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\",\n" +
                "      \"~toLabels\" : \"endLabel\"\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("(_)-edgeLabel-(endLabel)", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseEdgeLabelFromJsonWithMissingEndVertexLabel() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\",\n" +
                "      \"~fromLabels\" : [ \"startLabel\" ]\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("(startLabel)-edgeLabel-(_)", label.fullyQualifiedLabel());
    }

    @Test
    public void shouldParseEdgeLabelFromJsonWithMissingStartAndEndVertexLabels() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\"\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("edgeLabel", label.fullyQualifiedLabel());
    }

}