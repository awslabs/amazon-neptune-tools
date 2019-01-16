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

package com.amazonaws.services.neptune.io;

import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class JsonPrinterTest {

    @Test
    public void shouldPrintEdge() throws Exception {

        StringWriter stringWriter = new StringWriter();

        try (Printer printer = Format.json.createPrinter(new PrintWriter(stringWriter), new HashMap<>())) {
            printer.printStartRow();
            printer.printEdge("edge-id", "edge-label", "from-id", "to-id");
            printer.printEndRow();
        }

        assertEquals(
                "{\"~id\":\"edge-id\",\"~label\":\"edge-label\",\"~from\":\"from-id\",\"~to\":\"to-id\"}",
                stringWriter.toString());
    }

}