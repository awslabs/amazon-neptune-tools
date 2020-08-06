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

import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.propertygraph.metadata.DataType;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class CsvPropertyGraphPrinter implements PropertyGraphPrinter {

    private final OutputWriter writer;
    private final Map<Object, PropertyTypeInfo> metadata;
    private final CommaPrinter commaPrinter;
    private final boolean includeHeaders;
    private final boolean includeTypeDefinitions;

    public CsvPropertyGraphPrinter(OutputWriter writer,
                                   Map<Object, PropertyTypeInfo> metadata,
                                   boolean includeHeaders,
                                   boolean includeTypeDefinitions) {
        this.writer = writer;
        this.metadata = metadata;
        this.commaPrinter = new CommaPrinter(writer);
        this.includeHeaders = includeHeaders;
        this.includeTypeDefinitions = includeTypeDefinitions;
    }

    @Override
    public void printHeaderMandatoryColumns(String... columns) {
        if (includeHeaders) {
            for (String column : columns) {
                commaPrinter.printComma();
                writer.print(column);
            }
        }
    }

    @Override
    public void printHeaderRemainingColumns(Collection<PropertyTypeInfo> remainingColumns) {
        if (includeHeaders) {
            for (PropertyTypeInfo property : remainingColumns) {
                commaPrinter.printComma();
                if (includeTypeDefinitions) {
                    writer.print(property.nameWithDataType());
                } else {
                    writer.print(property.nameWithoutDataType());
                }
            }
            writer.print(System.lineSeparator());
        }
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
                writer.print(formattedValue);

            } else {
                commaPrinter.printComma();
            }
        }
    }

    @Override
    public void printProperties(String id, String streamOperation, Map<?, ?> properties) throws IOException {
        printProperties(properties);
    }

    @Override
    public void printEdge(String id, String label, String from, String to) {
        commaPrinter.printComma();
        writer.print(DataType.String.format(id));
        commaPrinter.printComma();
        writer.print(DataType.String.format(label));
        commaPrinter.printComma();
        writer.print(DataType.String.format(from));
        commaPrinter.printComma();
        writer.print(DataType.String.format(to));
    }

    @Override
    public void printNode(String id, List<String> labels) {
        commaPrinter.printComma();
        writer.print(DataType.String.format(id));
        commaPrinter.printComma();
        writer.print(DataType.String.formatList(labels));
    }

    @Override
    public void printStartRow() {
        writer.startCommit();
        commaPrinter.init();
    }

    @Override
    public void printEndRow() {
        writer.print(System.lineSeparator());
        writer.endCommit();
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
        writer.close();
    }

    private static class CommaPrinter {
        private final OutputWriter outputWriter;
        private boolean printComma = false;

        private CommaPrinter(OutputWriter outputWriter) {
            this.outputWriter = outputWriter;
        }

        void printComma() {
            if (printComma) {
                outputWriter.print(",");
            } else {
                printComma = true;
            }
        }

        void init() {
            printComma = false;
        }
    }
}
