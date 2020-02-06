/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import org.junit.Test;

import static org.junit.Assert.*;

public class MultiValuedNodePropertyPolicyTest {

    @Test
    public void shouldReturnStringPropertyValueIfPolicyIsLeaveAsString() {

        String value = "[\"toy\",\"electronics\",\"gifts\"]";

        PropertyValueParser parser = new PropertyValueParser(MultiValuedNodePropertyPolicy.LeaveAsString, "", false);
        PropertyValue propertyValue = parser.parse(value);

        assertEquals("\"[\"\"toy\"\",\"\"electronics\"\",\"\"gifts\"\"]\"", propertyValue.value());
        assertFalse(propertyValue.isMultiValued());
    }

    @Test
    public void shouldThrowExceptionIfPolicyIsHalt() {
        String value = "[\"toy\",\"electronics\",\"gifts\"]";

        PropertyValueParser parser = new PropertyValueParser(MultiValuedNodePropertyPolicy.Halt, "", false);

        try {
            parser.parse(value);
            fail();
        } catch (RuntimeException e) {
            assertEquals("Halt: found multivalued node property value", e.getMessage());
        }
    }

    @Test
    public void shouldReturnSetFormattedPropertyValueIfPolicyIsPutInSetIgnoringDuplicates() {

        String value = "[\"toy\",\"electronics\",\"gifts\"]";

        PropertyValueParser parser = new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, "", false);
        PropertyValue propertyValue = parser.parse(value);

        assertEquals("electronics;toy;gifts", propertyValue.value());
        assertTrue(propertyValue.isMultiValued());
    }

    @Test
    public void shouldThrowExceptionIfPolicyIsPutInSetButHaltIfDuplicates() {
        String value = "[\"toy\",\"electronics\",\"gifts\",\"gifts\"]";

        PropertyValueParser parser = new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetButHaltIfDuplicates, "", false);

        try {
            parser.parse(value);
            fail();
        } catch (RuntimeException e) {
            assertEquals("Halt: found multivalued node property value with duplicate values", e.getMessage());
        }
    }

}