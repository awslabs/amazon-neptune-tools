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

import com.amazonaws.services.neptune.propertygraph.metadata.DataType;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvPrinter implements Printer {

    private final PrintWriter printer;
    private final Map<Object, PropertyTypeInfo> metadata;
    private final CommaPrinter commaPrinter;

    public CsvPrinter(PrintWriter printer, Map<Object, PropertyTypeInfo> metadata) {
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
        for (Map.Entry<Object, PropertyTypeInfo> entry : metadata.entrySet()) {

            Object property = entry.getKey();
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
