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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.PrintOutputWriter;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import static com.amazonaws.services.neptune.util.MapUtils.entry;
import static com.amazonaws.services.neptune.util.MapUtils.map;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class JsonPropertyGraphPrinterTest {

    @Test
    public void shouldPrintEdge() throws Exception {

        StringWriter stringWriter = new StringWriter();

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(
                new PrintOutputWriter("test", stringWriter),
                new LabelSchema(new Label("my-label")),
                PrinterOptions.NO_HEADERS)) {

            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printEdge("edge-id", "edge-label", "from-id", "to-id");
            propertyGraphPrinter.printEndRow();

        }

        assertEquals(
                "{\"~id\":\"edge-id\",\"~label\":\"edge-label\",\"~from\":\"from-id\",\"~to\":\"to-id\"}",
                stringWriter.toString());
    }

    @Test
    public void appendsPreviouslyUnseenValuesToObjectWhenInferringSchema() throws IOException {

        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NO_HEADERS);

        print(printer,
                map(entry("fname", "fname1")),
                map(entry("fname", "fname2"), entry("lname", "lname2")),
                map(entry("fname", "fname3"), entry("age", 30)),
                map(entry("lname", "lname4"), entry("age", 40)),
                map(entry("fname", "fname5"), entry("lname", "lname5"), entry("age", 50))
        );

        String expectedOutput = "{\"fname\":\"fname1\"}\n" +
                "{\"fname\":\"fname2\",\"lname\":\"lname2\"}\n" +
                "{\"fname\":\"fname3\",\"age\":30}\n" +
                "{\"lname\":\"lname4\",\"age\":40}\n" +
                "{\"fname\":\"fname5\",\"lname\":\"lname5\",\"age\":50}";

        assertEquals(expectedOutput, stringWriter.toString());

    }

    @Test
    public void updatesDataTypesForColumnsWithEachNewRowWhenInferringSchema() throws IOException {
        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NO_HEADERS);

        print(printer,
                map(entry("age", 10)),
                map(entry("age", "ten"), entry("height", 5)),
                map(entry("age", 11), entry("height", 5.2))
        );

        assertEquals(2, labelSchema.propertyCount());
        assertEquals(DataType.String, labelSchema.getPropertySchema("age").dataType());
    }

    @Test
    public void columnsThatDoNotAppearInFirstRowAreNullable() throws IOException {
        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NO_HEADERS);

        print(printer,
                map(entry("p-1", 10), entry("p-2", 20)),
                map(entry("p-1", 30), entry("p-2", 40), entry("p-3", 50)),
                map(entry("p-1", 60), entry("p-2", 70), entry("p-4", 80))
        );

        assertFalse(labelSchema.getPropertySchema("p-1").isNullable());
        assertFalse(labelSchema.getPropertySchema("p-2").isNullable());
        assertTrue(labelSchema.getPropertySchema("p-3").isNullable());
        assertTrue(labelSchema.getPropertySchema("p-4").isNullable());
    }

    @Test
    public void columnsThatAppearInFirstRowButNotSubsequentRowsAreNullable() throws IOException {
        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NO_HEADERS);

        print(printer,
                map(entry("p-1", 10), entry("p-2", 20)),
                map(entry("p-2", 40), entry("p-3", 50)),
                map(entry("p-1", 60), entry("p-2", 70), entry("p-4", 80))
        );

        assertTrue(labelSchema.getPropertySchema("p-1").isNullable());
        assertFalse(labelSchema.getPropertySchema("p-2").isNullable());
        assertTrue(labelSchema.getPropertySchema("p-3").isNullable());
        assertTrue(labelSchema.getPropertySchema("p-4").isNullable());
    }

    private void print(PropertyGraphPrinter printer, Map<?, ?>... rows) throws IOException {
        for (Map<?, ?> row : rows) {
            printer.printStartRow();
            printer.printProperties(row);
            printer.printEndRow();
        }
    }
}