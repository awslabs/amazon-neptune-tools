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
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.ImputerTypeV2;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParseImputerTypeV2Test {

    @Test
    public void throwsErrorIfInvalidImputer(){

        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("imputer", "invalid");

        try{
            new ParseImputerTypeV2(json, new ParsingContext("context")).parseImputerType();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e){
            assertEquals("Invalid 'imputer' value for context: 'invalid'. Valid values are: 'mean', 'median', 'most-frequent'.", e.getMessage());
        }
    }

    @Test
    public void returnsNoneIfImputerMissing(){
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        ImputerTypeV2 imputerType = new ParseImputerTypeV2(json, new ParsingContext("context")).parseImputerType();

        assertEquals(imputerType, ImputerTypeV2.none);
    }
}