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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
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
    public void shouldRoundTripDateWhenCallingFormatWithEscapeNewlineParam() {
        Date now = new Date();
        DataType dataType = DataType.dataTypeFor(now.getClass());
        String nowString = dataType.format(now, false);
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

    @Test
    public void doubleShouldWriteIntAsDouble() throws IOException {

        String result1 = createJsonArray(generator -> DataType.Double.printTo(generator, 0));
        assertEquals("[0.0]", result1);

        String result2 = createJsonObject(generator -> DataType.Double.printTo(generator, "value", 0));
        assertEquals("{\"value\":0.0}", result2);
    }

    @Test
    public void longShouldWriteIntAsLong() throws IOException {

        String result1 = createJsonArray(generator -> DataType.Long.printTo(generator, 0));
        assertEquals("[0]", result1);

        String result2 = createJsonObject(generator -> DataType.Long.printTo(generator, "value", 0));
        assertEquals("{\"value\":0}", result2);
    }

    @Test
    public void floatShouldWriteIntAsFloat() throws IOException {

        String result1 = createJsonArray(generator -> DataType.Float.printTo(generator, 0));
        assertEquals("[0.0]", result1);

        String result2 = createJsonObject(generator -> DataType.Float.printTo(generator, "value", 0));
        assertEquals("{\"value\":0.0}", result2);
    }

    @Test
    public void shortShouldWriteIntAsShort() throws IOException {

        String result1 = createJsonArray(generator -> DataType.Short.printTo(generator, 0));
        assertEquals("[0]", result1);

        String result2 = createJsonObject(generator -> DataType.Short.printTo(generator, "value", 0));
        assertEquals("{\"value\":0}", result2);
    }

    @Test
    public void byteShouldWriteIntAsByte() throws IOException {

        String result1 = createJsonArray(generator -> DataType.Byte.printTo(generator, 0));
        assertEquals("[0]", result1);

        String result2 = createJsonObject(generator -> DataType.Byte.printTo(generator, "value", 0));
        assertEquals("{\"value\":0}", result2);
    }

    @Test
    public void boolShouldWriteIntAsBool() throws IOException {

        String result1 = createJsonArray(generator -> DataType.Boolean.printTo(generator, 0));
        assertEquals("[false]", result1);

        String result2 = createJsonObject(generator -> DataType.Boolean.printTo(generator, "value", 0));
        assertEquals("{\"value\":false}", result2);
    }


    private String createJsonArray(UseDataType useDataType) throws IOException {
        StringWriter writer = new StringWriter();
        JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
        jsonGenerator.writeStartArray();

        useDataType.apply(jsonGenerator);

        jsonGenerator.writeEndArray();
        jsonGenerator.flush();

        return writer.toString();
    }

    private String createJsonObject(UseDataType useDataType) throws IOException {
        StringWriter writer = new StringWriter();
        JsonGenerator jsonGenerator = new JsonFactory().createGenerator(writer);
        jsonGenerator.writeStartObject();

        useDataType.apply(jsonGenerator);

        jsonGenerator.writeEndObject();
        jsonGenerator.flush();

        return writer.toString();
    }

    private interface UseDataType{
        void apply(JsonGenerator generator) throws IOException;
    }
}