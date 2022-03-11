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
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JsonPropertyGraphPrinter implements PropertyGraphPrinter {

    private final OutputWriter writer;
    private final JsonGenerator generator;
    private final LabelSchema labelSchema;
    private final boolean allowUpdateSchema;
    private final PrinterOptions printerOptions;
    private boolean isNullable = false;
    private final TokenPrefix tokenPrefix;

    public JsonPropertyGraphPrinter(OutputWriter writer, JsonGenerator generator, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
        this(writer, generator, labelSchema, printerOptions, false);

    }

    public JsonPropertyGraphPrinter(OutputWriter writer, JsonGenerator generator, LabelSchema labelSchema, PrinterOptions printerOptions, boolean allowUpdateSchema) throws IOException {
        this.writer = writer;
        this.generator = generator;
        this.labelSchema = labelSchema;
        this.allowUpdateSchema = allowUpdateSchema;
        this.printerOptions = printerOptions;
        this.tokenPrefix = printerOptions.json().tokenPrefix();
    }

    @Override
    public String outputId() {
        return writer.outputId();
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
    public void printProperties(Map<?, ?> properties) throws IOException {

        // print known properties
        for (PropertySchema propertySchema : labelSchema.propertySchemas()) {

            Object key = propertySchema.property();
            Object value = properties.get(key);

            if (properties.containsKey(key)) {
                int size = propertySchema.accept(value, allowUpdateSchema);
                labelSchema.recordObservation(propertySchema, value, size);
                printProperty(value, propertySchema);
            } else {
                if (allowUpdateSchema) {
                    propertySchema.makeNullable();
                }
            }
        }

        // Print unknown properties
        if (allowUpdateSchema) {
            for (Map.Entry<?, ?> property : properties.entrySet()) {

                Object key = property.getKey();

                if (!labelSchema.containsProperty(key)) {

                    Object value = property.getValue();

                    PropertySchema propertySchema = new PropertySchema(key);
                    int size = propertySchema.accept(value, true);
                    if (isNullable) {
                        propertySchema.makeNullable();
                    }

                    labelSchema.put(key, propertySchema);
                    labelSchema.recordObservation(propertySchema, value, size);

                    printProperty(value, propertySchema);
                }
            }
        }

        isNullable = true;

    }

    private void printProperty(Object value, PropertySchema propertySchema) throws IOException {
        String formattedKey = propertySchema.nameWithoutDataType();

        if (isMap(value)) {
            generator.writeFieldName(formattedKey);
            printStartRow();
            printNestedProperties((Map<?, ?>) value);
            printEndRow();
        } else {
            DataType dataType = propertySchema.dataType();
            boolean isMultiValue = propertySchema.isMultiValue();

            printProperty(value, dataType, formattedKey, isMultiValue);
        }
    }

    private void printNestedProperties(Map<?, ?> value) throws IOException {
        for (Map.Entry<?, ?> property : value.entrySet()) {
            PropertySchema propertySchema = new PropertySchema(property.getKey());
            propertySchema.accept(property.getValue(), true);
            printProperty(property.getValue(), propertySchema);
        }
    }

    private void printProperty(Object value, DataType dataType, String formattedKey, boolean forceMultiValue) throws IOException {

        if (forceMultiValue) {

            List<?> values = isList(value) ? (List<?>) value : Collections.singletonList(value);

            generator.writeFieldName(formattedKey);
            generator.writeStartArray();
            for (Object v : values) {
                dataType.printTo(generator, v);
            }
            generator.writeEndArray();

        } else {
            if (isList(value)) {
                List<?> values = (List<?>) value;
                if (values.size() != 1 || printerOptions.json().strictCardinality()) {
                    generator.writeFieldName(formattedKey);
                    generator.writeStartArray();
                    for (Object v : values) {
                        dataType.printTo(generator, v);
                    }
                    generator.writeEndArray();
                } else {
                    dataType.printTo(generator, formattedKey, values.get(0));
                }
            } else {
                dataType.printTo(generator, formattedKey, value);
            }
        }
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
        generator.writeStringField( tokenPrefix.format("id"), id);
        generator.writeStringField(tokenPrefix.format("label"), label);
        generator.writeStringField(tokenPrefix.format("from"), from);
        generator.writeStringField(tokenPrefix.format("to"), to);
        if (fromLabels != null) {
            printProperty(fromLabels, DataType.String, tokenPrefix.format("fromLabels"), true);
        }
        if (toLabels != null) {
            printProperty(toLabels, DataType.String, tokenPrefix.format("toLabels"), true);
        }
    }

    @Override
    public void printNode(String id, List<String> labels) throws IOException {
        generator.writeStringField(tokenPrefix.format("id"), id);
        printProperty(labels, DataType.String, tokenPrefix.format("label"), true);
    }

    @Override
    public void printStartRow() throws IOException {
        writer.startCommit();
        generator.writeStartObject();
    }

    @Override
    public void printEndRow() throws IOException {
        generator.writeEndObject();
        generator.flush();
        writer.endCommit();
    }

    @Override
    public void close() throws Exception {
        generator.close();
        writer.close();
    }

    private boolean isList(Object value) {
        return value instanceof List<?>;
    }

    private boolean isMap(Object value) {
        return value instanceof Map<?, ?>;
    }
}
