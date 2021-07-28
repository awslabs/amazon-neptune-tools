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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParseNodeTypeTest {

    @Test
    public void canParseSimpleLabel() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("node", "Person");

        Label label = new ParseNodeType(json, new ParsingContext("node")).parseNodeType();
        assertEquals("Person", label.fullyQualifiedLabel());
    }

    @Test
    public void canParseMultiLabel() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.add("Person");
        arrayNode.add("Admin");

        json.set("node", arrayNode);

        Label label = new ParseNodeType(json, new ParsingContext("node")).parseNodeType();
        assertEquals("Admin;Person", label.fullyQualifiedLabel());
    }

    @Test
    public void canParseSemicolonSeparatedMultiLabel() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();

        json.put("node", "Person;Admin");

        Label label = new ParseNodeType(json, new ParsingContext("node")).parseNodeType();
        assertEquals("Admin;Person", label.fullyQualifiedLabel());
    }

    @Test
    public void throwsErrorIfNodeFieldIsMissing() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();

        try{
            new ParseNodeType(json, new ParsingContext("node")).parseNodeType();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e){
            assertEquals("Error parsing 'node' field for node. Expected a text value or array of text values.", e.getMessage());
        }
    }

    @Test
    public void throwsErrorIfNodeFieldIsNotText() {

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("node", 1);

        try{
            new ParseNodeType(json, new ParsingContext("node")).parseNodeType();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e){
            assertEquals("Error parsing 'node' field for node. Expected a text value or array of text values.", e.getMessage());
        }
    }

}