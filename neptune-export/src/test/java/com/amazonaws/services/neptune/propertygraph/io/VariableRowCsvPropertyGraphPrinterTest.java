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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.PrintOutputWriter;
import com.amazonaws.services.neptune.propertygraph.metadata.DataType;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyMetadataForLabel;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VariableRowCsvPropertyGraphPrinterTest {

    @Test
    public void appendsPreviouslyUnseenColumnsToEndOfRow() throws IOException {

        StringWriter stringWriter = new StringWriter();

        PropertyMetadataForLabel metadata = new PropertyMetadataForLabel();

        VariableRowCsvPropertyGraphPrinter printer = new VariableRowCsvPropertyGraphPrinter(
                new PrintOutputWriter("test", stringWriter),
                metadata);

        print(printer,
                row(entry("fname", "fname1")),
                row(entry("fname", "fname2"), entry("lname", "lname2")),
                row(entry("fname", "fname3"), entry("age", 30)),
                row(entry("lname", "lname4"), entry("age", 40)),
                row(entry("fname", "fname5"), entry("lname", "lname5"), entry("age", 50))
        );

        String expectedOutput = "\"fname1\"\n" +
                "\"fname2\",\"lname2\"\n" +
                "\"fname3\",,30\n" +
                ",\"lname4\",40\n" +
                "\"fname5\",\"lname5\",50\n";

        assertEquals(expectedOutput, stringWriter.toString());

    }

    @Test
    public void updatesDataTypesForColumsnWithEachNewRow() throws IOException {
        StringWriter stringWriter = new StringWriter();

        PropertyMetadataForLabel metadata = new PropertyMetadataForLabel();

        VariableRowCsvPropertyGraphPrinter printer = new VariableRowCsvPropertyGraphPrinter(
                new PrintOutputWriter("test", stringWriter),
                metadata);

        print(printer,
                row(entry("age", 10)),
                row(entry("age", "ten"), entry("height", 5)),
                row(entry("age", 11), entry("height", 5.2))
        );

        assertEquals(2, metadata.propertyCount());
        assertEquals(DataType.String, metadata.getPropertyTypeInfo("age").dataType());
        assertEquals(DataType.Double, metadata.getPropertyTypeInfo("height").dataType());
    }

    private void print(PropertyGraphPrinter printer, Map<?, ?>... rows) throws IOException {
        for (Map<?, ?> row : rows) {
            printer.printStartRow();
            printer.printProperties(row);
            printer.printEndRow();
        }
    }

    private Map<?, ?> row(Entry... entries) {
        HashMap<Object, Object> map = new HashMap<>();
        for (Entry entry : entries) {
            map.put(entry.key(), entry.value());
        }
        return map;
    }

    public Entry entry(String key, Object value) {
        return new Entry(key, value);
    }

    public static class Entry {

        private final String key;
        private final Object value;

        private Entry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public Object value() {
            return value;
        }
    }

}