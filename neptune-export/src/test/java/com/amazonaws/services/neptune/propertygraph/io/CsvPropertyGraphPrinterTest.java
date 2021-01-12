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
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import org.junit.Test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CsvPropertyGraphPrinterTest {

    @Test
    public void shouldUseSeparatorToSeparateMultipleValues() throws Exception {

        String separator = "|";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("X", "Y"));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().setMultiValueSeparator(separator).build()));

        printer.printProperties(props);

        assertEquals(
                "\"X|Y\"",
                stringWriter.toString());
    }

    @Test
    public void shouldEscapeSeparatorValuesInMultipleValues() throws Exception {

        String separator = "|";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("A|B", "Y"));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().setMultiValueSeparator(separator).build()));

        printer.printProperties(props);

        assertEquals(
                "\"A\\|B|Y\"",
                stringWriter.toString());
    }

    @Test
    public void shouldUseEmptySeparatorToSeparateMultipleValues() throws Exception {

        String separator = "";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("X;B", "Y"));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().setMultiValueSeparator(separator).build()));

        printer.printProperties(props);

        assertEquals(
                "\"X;BY\"",
                stringWriter.toString());
    }

}