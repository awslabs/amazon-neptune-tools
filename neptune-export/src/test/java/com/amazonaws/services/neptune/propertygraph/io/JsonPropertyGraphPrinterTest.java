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
import com.amazonaws.services.neptune.propertygraph.metadata.DataType;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import java.io.StringWriter;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class JsonPropertyGraphPrinterTest {

    @Test
    public void shouldPrintEdge() throws Exception {

        StringWriter stringWriter = new StringWriter();

        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter(stringWriter), new HashMap<>(), true)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printEdge("edge-id", "edge-label", "from-id", "to-id");
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
                "{\"~id\":\"edge-id\",\"~label\":\"edge-label\",\"~from\":\"from-id\",\"~to\":\"to-id\"}",
                stringWriter.toString());
    }

    @Test
    public void shouldPrintEmptyListAsList() throws Exception {
        StringWriter stringWriter = new StringWriter();
        PropertyTypeInfo emptyList = new PropertyTypeInfo("empty-list", DataType.String, true);
        HashMap<Object, PropertyTypeInfo>  propTypeInfo = new HashMap<Object, PropertyTypeInfo>() {{
            put("empty-list", emptyList);
        }};

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("empty-list", new ArrayList<String>());
        }};
        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter(stringWriter), propTypeInfo, true)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
            "{\"empty-list\":[]}",
            stringWriter.toString());
    }

    @Test
    public void shouldPrintSingleElementListAsList() throws Exception {
        StringWriter stringWriter = new StringWriter();
        PropertyTypeInfo singleList = new PropertyTypeInfo("single-list", DataType.String, true);
        HashMap<Object, PropertyTypeInfo>  propTypeInfo = new HashMap<Object, PropertyTypeInfo>() {{
            put("single-list", singleList);
        }};

        ArrayList<String> elems = new ArrayList<String>() {{ add("the_element"); }};
        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("single-list", elems);
        }};
        try (PropertyGraphPrinter propertyGraphPrinter = PropertyGraphExportFormat.json.createPrinter(new PrintOutputWriter(stringWriter), propTypeInfo, true)) {
            propertyGraphPrinter.printStartRow();
            propertyGraphPrinter.printProperties(props);
            propertyGraphPrinter.printEndRow();
        }

        assertEquals(
            "{\"single-list\":[\"the_element\"]}",
            stringWriter.toString());
    }
}
