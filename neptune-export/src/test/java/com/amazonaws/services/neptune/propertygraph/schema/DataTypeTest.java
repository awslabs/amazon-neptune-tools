/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph.schema;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class DataTypeTest {

    @Test
    public void emptyStringDateValueShouldReturnEmptyString() {
        String result = DataType.Date.format("");
        assertEquals("", result);
    }

    @Test
    public void shouldEscapeDoubleQuotes() {
        String result = DataType.String.format("One \"two\" three");
        assertEquals("\"One \"\"two\"\" three\"", result);
    }

    @Test
    public void shouldNotDoubleEscapeDoubleQuotesThatHaveAlreadyBeenEscaped() {
        String result = DataType.String.format("One \"\"two\"\" three");
        assertEquals("\"One \"\"two\"\" three\"", result);
    }

    @Test
    public void shouldRoundTripDate() {
        Date now = new Date();
        DataType dataType = DataType.dataTypeFor(now.getClass());
        String nowString = dataType.format(now);
        Object converted = dataType.convert(nowString);
        assertEquals(now, converted);
    }

    @Test
    public void shouldNotEscapeNewlineChar(){
        String result = DataType.String.format("A\nB");
        assertEquals("\"A\nB\"", result);
    }

    @Test
    public void shouldNotEscapeNewline(){
        String result = DataType.String.format("A" + System.lineSeparator() + "B");
        assertEquals("\"A\nB\"", result);
    }

    @Test
    public void shouldEscapeNewlineCharIfEscapeNewlineSetToTrue(){
        String result = DataType.String.format("A\nB", true);
        assertEquals("\"A\\nB\"", result);
    }

    @Test
    public void shouldEscapeNewlineIfEscapeNewlineSetToTrue(){
        String result = DataType.String.format("A" + System.lineSeparator() + "B", true);
        assertEquals("\"A\\nB\"", result);
    }
}