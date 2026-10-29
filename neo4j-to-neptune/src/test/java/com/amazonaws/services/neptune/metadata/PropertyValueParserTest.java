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

public class PropertyValueParserTest {

    @Test
    public void shouldDoubleUpDoubleQuotesAndSurroundWithDoubleQuotesAStringValueContainingDoubleQuotes() {
        String originalValue = "First \"second\" third fourth";
        PropertyValue propertyValue = new PropertyValueParser(MultiValuedNodePropertyPolicy.LeaveAsString, "", false).parse(originalValue);

        assertEquals("\"First \"\"second\"\" third fourth\"", propertyValue.value());
        assertFalse(propertyValue.isMultiValued());
    }

    @Test
    public void shouldSurroundWithDoubleQuotesAStringValueContainingComma() {
        String originalValue = "one, two, three";
        PropertyValue propertyValue = new PropertyValueParser(MultiValuedNodePropertyPolicy.LeaveAsString, "", false).parse(originalValue);

        assertEquals("\"one, two, three\"", propertyValue.value());
        assertFalse(propertyValue.isMultiValued());
    }

    @Test
    public void shouldSurroundWithDoubleQuotesAStringValueContainingNewLine() {
        String originalValue = "one" + System.lineSeparator() + "two";
        PropertyValue propertyValue = new PropertyValueParser(MultiValuedNodePropertyPolicy.LeaveAsString, "", false).parse(originalValue);

        assertEquals("\"one" + System.lineSeparator() + "two\"", propertyValue.value());
        assertFalse(propertyValue.isMultiValued());
    }

    @Test
    public void shouldReplaceSemicolonInMultiValuePropertiesWithReplacementString() {
        String originalValue = "[\"one\",\"two;three\"]";
        PropertyValue propertyValue = new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, "SEMICOLON", false).parse(originalValue);

        assertEquals("one;twoSEMICOLONthree", propertyValue.value());
        assertTrue(propertyValue.isMultiValued());
    }

    @Test
    public void shouldQuoteCompleteMultiValuePropertyWhenAnElementContainsCommas() {
        String originalValue = "[\"A1ADVO\",\"Total Display, Video, and Audio (incl. SD/STV)\"]";
        PropertyValue propertyValue = new PropertyValueParser(
            MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, "", false).parse(originalValue);

        assertEquals("\"A1ADVO;Total Display, Video, and Audio (incl. SD/STV)\"",
            propertyValue.value());
        assertTrue(propertyValue.isMultiValued());
    }

    @Test
    public void shouldJoinArrayAsDelimitedStringPreservingOrderAndDuplicates() {
        String originalValue = "[\"kpi definitions\",\"base kpi\",\"kpi metadata\",\"base kpi\"]";
        PropertyValue propertyValue = new PropertyValueParser(
            MultiValuedNodePropertyPolicy.JoinWithDelimiter, "", true, "|", false, false)
            .parse(originalValue);

        assertEquals("kpi definitions|base kpi|kpi metadata|base kpi", propertyValue.value());
        assertFalse(propertyValue.isMultiValued());
        assertEquals(DataType.String, propertyValue.dataType());
    }

    @Test
    public void shouldAppendDelimiterAndQuoteJoinedStringWhenRequired() {
        String originalValue = "[\"A1ADVO\",\"Total Display, Video, and Audio (incl. SD/STV)\"]";
        PropertyValue propertyValue = new PropertyValueParser(
            MultiValuedNodePropertyPolicy.JoinWithDelimiter, "", true, "|", true, true)
            .parse(originalValue);

        assertEquals("\"A1ADVO|Total Display, Video, and Audio (incl. SD/STV)|\"",
            propertyValue.value());
        assertFalse(propertyValue.isMultiValued());
        assertEquals(DataType.String, propertyValue.dataType());
    }

    @Test
    public void shouldRejectJoinedStringWhenAValueContainsTheDelimiter() {
        String originalValue = "[\"first\",\"contains|delimiter\"]";
        PropertyValueParser parser = new PropertyValueParser(
            MultiValuedNodePropertyPolicy.JoinWithDelimiter, "", true, "|", false, false);

        try {
            parser.parse(originalValue);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("List value contains configured delimiter '|': contains|delimiter",
                e.getMessage());
        }
    }

    @Test
    public void shouldInferDateTimeWhenEnabled() {
        PropertyValue propertyValue = new PropertyValueParser(
            MultiValuedNodePropertyPolicy.LeaveAsString, "", true, "|", false, true)
            .parse("2026-09-14T19:53:51.367Z");

        assertEquals(DataType.DateTime, propertyValue.dataType());
    }

}
