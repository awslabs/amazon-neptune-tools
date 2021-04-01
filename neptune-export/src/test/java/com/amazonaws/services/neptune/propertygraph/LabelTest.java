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

import java.util.Arrays;

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
    public void shouldParseEdgeLabelWithMultiLabelStartAndEndVerticesFromJson() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\",\n" +
                "      \"~fromLabels\" : [ \"startLabel2\", \"startLabel1\" ],\n" +
                "      \"~toLabels\" : [ \"endLabel2\", \"endLabel1\", \"endLabel3\" ]\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("(startLabel1;startLabel2)-edgeLabel-(endLabel1;endLabel2;endLabel3)", label.fullyQualifiedLabel());
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
    public void shouldParseEdgeLabelFromJsonWithSemicolonSeparatedStringStartAndEndVertexLabels() throws JsonProcessingException {
        String json = "{\n" +
                "    \"label\" : {\n" +
                "      \"~label\" : \"edgeLabel\",\n" +
                "      \"~fromLabels\" : \"startLabel2;startLabel1\",\n" +
                "      \"~toLabels\" : \"endLabel2;endLabel1;endLabel3\"\n" +
                "    }\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Label label = Label.fromJson(jsonNode.path("label"));

        assertEquals("(startLabel1;startLabel2)-edgeLabel-(endLabel1;endLabel2;endLabel3)", label.fullyQualifiedLabel());
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

    @Test
    public void twoSimpleLabelsCanBeAssignedFromEachOther(){
        Label l1 = new Label("my-label");
        Label l2 = new Label("my-label");

        assertTrue(l1.isAssignableFrom(l2));
        assertTrue(l2.isAssignableFrom(l1));
    }

    @Test
    public void twoEquivalentComplexLabelsCanBeAssignedFromEachOther(){
        Label l1 = new Label("my-label", "startLabel1;startLabel2", "endLabel1;endLabel2");
        Label l2 = new Label("my-label", "startLabel1;startLabel2", "endLabel1;endLabel2");
        Label l3 = new Label("my-label", "startLabel2;startLabel1", "endLabel2;endLabel1");

        assertTrue(l1.isAssignableFrom(l2));
        assertTrue(l1.isAssignableFrom(l3));
        assertTrue(l2.isAssignableFrom(l1));
        assertTrue(l2.isAssignableFrom(l3));
        assertTrue(l3.isAssignableFrom(l1));
        assertTrue(l3.isAssignableFrom(l2));
    }

    @Test
    public void simpleLabelCanBeAssignedFromComplexLabelButComplexLabelCannotBeAssignedFromSimpleLabel(){
        Label l1 = new Label("my-label");
        Label l2 = new Label("my-label", "startLabel", "endLabel");

        assertTrue(l1.isAssignableFrom(l2));
        assertFalse(l2.isAssignableFrom(l1));
    }

    @Test
    public void complexLabelComprisingSubsetOfAnotherComplexLabelCanBeAssignedFromLatter(){
        Label l1 = new Label("my-label", "startLabel1", "endLabel1");
        Label l2 = new Label("my-label", "startLabel1", "");
        Label l3 = new Label("my-label", Arrays.asList("startLabel2", "startLabel1"), Arrays.asList("endLabel2", "endLabel1"));

        assertTrue(l1.isAssignableFrom(l3));
        assertTrue(l2.isAssignableFrom(l3));
        assertTrue(l2.isAssignableFrom(l1));

        assertFalse(l3.isAssignableFrom(l1));
        assertFalse(l3.isAssignableFrom(l2));
        assertFalse(l1.isAssignableFrom(l2));
    }

    @Test
    public void complexLabelsThatOnlyOverlapCannotBeAssignedFromEachOther(){
        Label l1 = new Label("my-label", "startLabel1, startLabel2", "endLabel1, endLabel2");
        Label l2 = new Label("my-label", "startLabel2, startLabel3", "endLabel2, endLabel3");

        assertFalse(l1.isAssignableFrom(l2));
        assertFalse(l2.isAssignableFrom(l1));
    }


}