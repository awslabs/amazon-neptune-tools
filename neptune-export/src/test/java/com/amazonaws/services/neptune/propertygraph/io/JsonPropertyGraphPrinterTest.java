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
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

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
                PrinterOptions.NULL_OPTIONS)) {

            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printEdge("edge-id", "edge-label", "from-id", "to-id");
            propertyGraphPrinter.printEndRow();

        }

        assertEquals(
                "{\"~id\":\"edge-id\",\"~label\":\"edge-label\",\"~from\":\"from-id\",\"~to\":\"to-id\"}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintEmptyListAsListIrrespectiveOfWhetherMultiValueIsTrue() throws Exception {
        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);
        PropertySchema propertySchema2 = new PropertySchema("property2", false, DataType.String, false);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);
        labelSchema.put("property2", propertySchema2);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", new ArrayList<>());
            put("property2", new ArrayList<>());
        }};

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter("outputId", stringWriter), labelSchema, PrinterOptions.NULL_OPTIONS)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"property1\":[],\"property2\":[]}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintSingleValueListAsSingleValueWhenIsMultiValueIsFalse() throws Exception {
        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema = new PropertySchema("tags", false, DataType.String, false);
        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("tags", propertySchema);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("tags", Collections.singletonList("tag1"));
        }};

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter("outputId", stringWriter), labelSchema, PrinterOptions.NULL_OPTIONS)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"tags\":\"tag1\"}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintSingleValueListAsSingleValueWhenIsMultiValueIsFalseButStrictCardinalityIsEnforced() throws Exception {
        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema = new PropertySchema("tags", false, DataType.String, false);
        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("tags", propertySchema);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("tags", Collections.singletonList("tag1"));
        }};

        PrinterOptions printerOptions = new PrinterOptions(JsonPrinterOptions.builder().setStrictCardinality(true).build());

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter("outputId", stringWriter), labelSchema, printerOptions)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"tags\":[\"tag1\"]}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintSingleValueListAsArrayWhenIsMultiValueIsTrue() throws Exception {
        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema = new PropertySchema("tags", false, DataType.String, true);
        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("tags", propertySchema);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("tags", Collections.singletonList("tag1"));
        }};

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter("outputId", stringWriter), labelSchema, PrinterOptions.NULL_OPTIONS)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"tags\":[\"tag1\"]}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintMultiValueListAsArrayIrrespectiveOfWhetherMultiValueIsTrue() throws Exception {
        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);
        PropertySchema propertySchema2 = new PropertySchema("property2", false, DataType.String, false);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);
        labelSchema.put("property2", propertySchema2);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("tag1", "tag2"));
            put("property2", Arrays.asList("tag1", "tag2"));
        }};

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter("outputId", stringWriter), labelSchema, PrinterOptions.NULL_OPTIONS)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"property1\":[\"tag1\",\"tag2\"],\"property2\":[\"tag1\",\"tag2\"]}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintNestedPropertiesMapAsJsonObject() throws Exception {
        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property", propertySchema1);

        Map<?, ?> props = map(entry("property", map(
                entry("nestedProperty1", "value1"),
                entry("nestedProperty2", "value2"))));

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter("outputId", stringWriter), labelSchema, PrinterOptions.NULL_OPTIONS)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"property\":{\"nestedProperty1\":\"value1\",\"nestedProperty2\":\"value2\"}}",
                stringWriter.toString());
    }

    @Test
    public void appendsPreviouslyUnseenValuesToObjectWhenInferringSchema() throws IOException {

        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NULL_OPTIONS);

        print(printer,
                map(entry("fname", "fname1")),
                map(entry("fname", "fname2"), entry("lname", "lname2")),
                map(entry("fname", "fname3"), entry("age", 30)),
                map(entry("lname", "lname4"), entry("age", 40)),
                map(entry("fname", "fname5"), entry("lname", "lname5"), entry("age", 50))
        );

        String expectedOutput = "{\"fname\":\"fname1\"}" + System.lineSeparator() +
                "{\"fname\":\"fname2\",\"lname\":\"lname2\"}" + System.lineSeparator() +
                "{\"fname\":\"fname3\",\"age\":30}" + System.lineSeparator() +
                "{\"lname\":\"lname4\",\"age\":40}" + System.lineSeparator() +
                "{\"fname\":\"fname5\",\"lname\":\"lname5\",\"age\":50}";

        assertEquals(expectedOutput, stringWriter.toString());

    }

    @Test
    public void updatesDataTypesInSchemaForColumnsWithEachNewRowWhenInferringSchema() throws IOException {
        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NULL_OPTIONS);

        print(printer,
                map(entry("age", 10)),
                map(entry("age", "ten"), entry("height", 5)),
                map(entry("age", 11), entry("height", 5.2))
        );

        assertEquals(2, labelSchema.propertyCount());
        assertEquals(DataType.String, labelSchema.getPropertySchema("age").dataType());
    }

    @Test
    @Ignore
    public void keepsOriginalDatatypesForPropertyValuesWhenWritingProperties() throws IOException {
        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NULL_OPTIONS);

        print(printer,
                map(entry("age", 10)),
                map(entry("age", "ten"), entry("height", 5)),
                map(entry("age", 11), entry("height", 5.2))
        );

        String expectedOutput = "{\"age\":10}\n" +
                "{\"age\":\"ten\",\"height\":5}\n" +
                "{\"age\":11,\"height\":5.2}";

        assertEquals(expectedOutput, stringWriter.toString());
    }

    @Test
    public void columnsThatDoNotAppearInFirstRowAreNullable() throws IOException {
        StringWriter stringWriter = new StringWriter();

        LabelSchema labelSchema = new LabelSchema(new Label("my-label"));

        PropertyGraphPrinter printer = PropertyGraphExportFormat.json.createPrinterForInferredSchema(
                new PrintOutputWriter("test", stringWriter),
                labelSchema,
                PrinterOptions.NULL_OPTIONS);

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
                PrinterOptions.NULL_OPTIONS);

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