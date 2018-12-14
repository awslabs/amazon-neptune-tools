package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.metadata.DataType;
import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvPrinter implements Printer {

    private final PrintWriter printer;
    private final Map<String, PropertyTypeInfo> metadata;
    private final CommaPrinter commaPrinter;

    public CsvPrinter(PrintWriter printer, Map<String, PropertyTypeInfo> metadata) {
        this.printer = printer;
        this.metadata = metadata;
        this.commaPrinter = new CommaPrinter(printer);
    }

    @Override
    public void printHeaderMandatoryColumns(String... columns) {
        printer.print(Arrays.stream(columns).collect(Collectors.joining(",")));
        commaPrinter.printComma();
    }

    @Override
    public void printHeaderRemainingColumns(Collection<PropertyTypeInfo> remainingColumns, boolean includeTypeDefinitions) {
        for (PropertyTypeInfo property : remainingColumns) {
            commaPrinter.printComma();
            if (includeTypeDefinitions) {
                printer.print(property.nameWithDataType());
            } else {
                printer.print(property.nameWithoutDataType());
            }
        }
        printer.print(System.lineSeparator());
    }

    @Override
    public void printProperties(Map<?, ?> properties) {
        for (Map.Entry<String, PropertyTypeInfo> entry : metadata.entrySet()) {

            String property = entry.getKey();
            DataType dataType = entry.getValue().dataType();

            if (properties.containsKey(property)) {
                commaPrinter.printComma();

                Object value = properties.get(property);
                String formattedValue = isList(value) ?
                        formatList(value, dataType) :
                        dataType.format(value);
                printer.print(formattedValue);

            } else {
                commaPrinter.printComma();
            }
        }
    }

    @Override
    public void printEdge(String id, String label, String from, String to) {
        printer.printf("%s,%s,%s,%s", id, label, from, to);
    }

    @Override
    public void printNode(String id, String label) {
        printer.printf("%s,%s", id, label);
    }

    @Override
    public void printStartRow() {
        // Do nothing
    }

    @Override
    public void printEndRow() {
        printer.print(System.lineSeparator());
    }

    private String formatList(Object value, DataType dataType) {
        List<?> values = (List<?>) value;
        return dataType.formatList(values);
    }

    private boolean isList(Object value) {
        return value.getClass().isAssignableFrom(java.util.ArrayList.class);
    }

    @Override
    public void close() throws Exception {
        printer.close();
    }

    private static class CommaPrinter {
        private final PrintWriter printer;
        private boolean printComma = false;

        private CommaPrinter(PrintWriter printer) {
            this.printer = printer;
        }

        void printComma() {
            if (printComma) {
                printer.print(",");
            } else {
                printComma = true;
            }
        }
    }
}
