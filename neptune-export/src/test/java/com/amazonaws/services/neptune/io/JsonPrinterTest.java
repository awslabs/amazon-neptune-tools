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