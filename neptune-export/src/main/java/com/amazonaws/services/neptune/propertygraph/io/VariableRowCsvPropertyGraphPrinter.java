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

import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class VariableRowCsvPropertyGraphPrinter implements PropertyGraphPrinter {

    private final CsvPropertyGraphPrinter csvPropertyGraphPrinter;
    private final OutputWriter writer;
    private final LabelSchema labelSchema;
    private boolean isNullable = false;

    public VariableRowCsvPropertyGraphPrinter(OutputWriter writer,
                                              LabelSchema labelSchema,
                                              PrinterOptions printerOptions) {

        CsvPrinterOptions csvPrinterOptions = CsvPrinterOptions.builder()
                .setMultiValueSeparator(printerOptions.csv().multiValueSeparator())
                .build();

        this.writer = writer;
        this.labelSchema = labelSchema;
        this.csvPropertyGraphPrinter = new CsvPropertyGraphPrinter(
                writer,
                labelSchema,
                new PrinterOptions(csvPrinterOptions),
                true);
    }

    @Override
    public String outputId() {
        return csvPropertyGraphPrinter.outputId();
    }

    @Override
    public void printHeaderMandatoryColumns(String... columns) {
        // Do nothing
    }

    @Override
    public void printHeaderRemainingColumns(Collection<PropertySchema> remainingColumns) {
        // Do nothing
    }

    @Override
    public void printProperties(Map<?, ?> properties) {

        // Print known properties
        csvPropertyGraphPrinter.printProperties(properties);

        // Check to see whether known properties are present
        for (PropertySchema propertySchema : labelSchema.propertySchemas()) {
            if (!properties.containsKey(propertySchema.property())) {
                propertySchema.makeNullable();
            }
        }

        // Print unknown properties
        for (Map.Entry<?, ?> property : properties.entrySet()) {

            Object key = property.getKey();

            if (!labelSchema.containsProperty(key)) {

                Object value = property.getValue();

                PropertySchema propertySchema = new PropertySchema(key);
                PropertySchema.PropertyValueMetadata propertyValueMetadata = propertySchema.accept(value, true);
                if (isNullable) {
                    propertySchema.makeNullable();
                }

                labelSchema.put(key, propertySchema);
                labelSchema.recordObservation(propertySchema, value, propertyValueMetadata);

                csvPropertyGraphPrinter.printProperty(propertySchema, value);
            }
        }

        isNullable = true;
    }

    @Override
    public void printProperties(Map<?, ?> properties, boolean applyFormatting) throws IOException {
        printProperties(properties);
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
        csvPropertyGraphPrinter.printEdge(id, label, from, to, fromLabels, toLabels);
    }

    @Override
    public void printNode(String id, List<String> labels) {
        csvPropertyGraphPrinter.printNode(id, labels);
    }

    @Override
    public void printStartRow() {
        csvPropertyGraphPrinter.printStartRow();
    }

    @Override
    public void printEndRow() {
        csvPropertyGraphPrinter.printEndRow();
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
