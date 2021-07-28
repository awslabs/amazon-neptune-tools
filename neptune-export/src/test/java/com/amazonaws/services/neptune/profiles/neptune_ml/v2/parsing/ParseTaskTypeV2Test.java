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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParseTaskTypeV2Test {

    @Test
    public void throwsErrorIfInvalidTaskType(){
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("task_type", "invalid");

        try{
            new ParseTaskTypeV2(json, new ParsingContext("context")).parseTaskType();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e){
            assertEquals("Invalid 'task_type' value for context: 'invalid'. Valid values are: 'link_prediction', 'node_classification', 'node_regression', 'edge_classification', 'edge_regression'.", e.getMessage());
        }
    }

    @Test
    public void throwsErrorIfMissingTaskType(){
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("not_a_task_type", "a_value");

        try{
            new ParseTaskTypeV2(json, new ParsingContext("context")).parseTaskType();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e){
            assertEquals("Error parsing 'task_type' field for context. Expected one of the following values: 'link_prediction', 'node_classification', 'node_regression', 'edge_classification', 'edge_regression'.", e.getMessage());
        }
    }

}