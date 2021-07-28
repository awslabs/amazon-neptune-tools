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
import com.amazonaws.services.neptune.propertygraph.TokenPrefix;
import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import com.amazonaws.services.neptune.util.SemicolonUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CsvPropertyGraphPrinter implements PropertyGraphPrinter {

    private final OutputWriter writer;
    private final LabelSchema labelSchema;
    private final PrinterOptions printerOptions;
    private final boolean allowUpdateSchema;
    private final CommaPrinter commaPrinter;

    public CsvPropertyGraphPrinter(OutputWriter writer,
                                   LabelSchema labelSchema,
                                   PrinterOptions printerOptions) {
        this(writer, labelSchema, printerOptions, false);
    }

    public CsvPropertyGraphPrinter(OutputWriter writer,
                                   LabelSchema labelSchema,
                                   PrinterOptions printerOptions,
                                   boolean allowUpdateSchema) {
        this.writer = writer;
        this.labelSchema = labelSchema;
        this.printerOptions = printerOptions;
        this.commaPrinter = new CommaPrinter(writer);
        this.allowUpdateSchema = allowUpdateSchema;
    }

    @Override
    public String outputId() {
        return writer.outputId();
    }

    @Override
    public void printHeaderMandatoryColumns(String... columns) {
        if (printerOptions.csv().includeHeaders() && writer.isNewTarget()) {
            TokenPrefix tokenPrefix = printerOptions.csv().tokenPrefix();
            for (String column : columns) {
                commaPrinter.printComma();
                writer.print(tokenPrefix.format(column));
            }
        }
    }

    @Override
    public void printHeaderRemainingColumns(Collection<PropertySchema> remainingColumns) {
        if (printerOptions.csv().includeHeaders() && writer.isNewTarget()) {
            for (PropertySchema property : remainingColumns) {
                commaPrinter.printComma();
                if (printerOptions.csv().includeTypeDefinitions()) {
                    writer.print(property.nameWithDataType(printerOptions.csv().escapeCsvHeaders()));
                } else {
                    writer.print(property.nameWithoutDataType(printerOptions.csv().escapeCsvHeaders()));
                }
            }
            writer.print(writer.lineSeparator());
        }
    }

    @Override
    public void printProperties(Map<?, ?> properties) {
        printProperties(properties, true);
    }

    @Override
    public void printProperties(Map<?, ?> properties, boolean applyFormatting) {
        for (PropertySchema propertySchema : labelSchema.propertySchemas()) {

            Object property = propertySchema.property();

            if (properties.containsKey(property)) {
                Object value = properties.get(property);
                int size = propertySchema.accept(value, allowUpdateSchema);
                labelSchema.recordObservation(propertySchema, value, size);
                printProperty(propertySchema, value, applyFormatting);
            } else {
                commaPrinter.printComma();
            }
        }
    }

    public void printProperty(PropertySchema schema, Object value) {
        printProperty(schema, value, true);
    }

    private void printProperty(PropertySchema schema, Object value, boolean applyFormatting) {

        DataType dataType = schema.dataType();

        commaPrinter.printComma();

        if (applyFormatting) {
            String formattedValue = isList(value) ?
                    formatList(value, dataType, printerOptions.csv()) :
                    dataType.format(value);
            writer.print(formattedValue);
        } else {
            if (dataType == DataType.String) {
                if (isSingleValueColumnWithSemicolonSeparator(schema)) {
                    writer.print(DataType.String.format(SemicolonUtils.unescape(value.toString())));
                } else {
                    writer.print(DataType.String.format(value));
                }
            } else {
                writer.print(String.valueOf(value));
            }
        }
    }

    private boolean isSingleValueColumnWithSemicolonSeparator(PropertySchema schema) {
        return !schema.isMultiValue() && printerOptions.csv().isSemicolonSeparator();
    }

    @Override
    public void printProperties(String id, String streamOperation, Map<?, ?> properties) throws IOException {
        printProperties(properties);
    }

    @Override
    public void printEdge(String id, String label, String from, String to) throws IOException {
        printEdge(id, label, from, to, null, null);
    }

    @Override
    public void printEdge(String id, String label, String from, String to, Collection<String> fromLabels, Collection<String> toLabels) throws IOException {
        commaPrinter.printComma();
        writer.print(DataType.String.format(id));
        commaPrinter.printComma();
        writer.print(DataType.String.format(label));
        commaPrinter.printComma();
        writer.print(DataType.String.format(from));
        commaPrinter.printComma();
        writer.print(DataType.String.format(to));
        if (fromLabels != null) {
            commaPrinter.printComma();
            writer.print(DataType.String.formatList(fromLabels, printerOptions.csv()));
        }
        if (toLabels != null) {
            commaPrinter.printComma();
            writer.print(DataType.String.formatList(toLabels, printerOptions.csv()));
        }
    }

    @Override
    public void printNode(String id, List<String> labels) {
        commaPrinter.printComma();
        writer.print(DataType.String.format(id));
        commaPrinter.printComma();
        writer.print(DataType.String.formatList(labels, printerOptions.csv()));
    }

    @Override
    public void printStartRow() {
        writer.startCommit();
        commaPrinter.init();
    }

    @Override
    public void printEndRow() {
        writer.print(writer.lineSeparator());
        writer.endCommit();
    }

    private String formatList(Object value, DataType dataType, CsvPrinterOptions options) {
        List<?> values = (List<?>) value;
        return dataType.formatList(values, options);
    }

    private boolean isList(Object value) {
        return value instanceof List<?>;
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }

}
