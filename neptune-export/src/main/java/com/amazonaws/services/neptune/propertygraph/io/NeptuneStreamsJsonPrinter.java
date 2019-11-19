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
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class NeptuneStreamsJsonPrinter implements Printer {

    private final OutputWriter writer;
    private final JsonGenerator generator;
    private final Map<Object, PropertyTypeInfo> metadata;

    public NeptuneStreamsJsonPrinter(OutputWriter writer,
                                     JsonGenerator generator,
                                     Map<Object, PropertyTypeInfo> metadata) throws IOException {
        this.writer = writer;
        this.generator = generator;
        this.metadata = metadata;
    }

    @Override
    public void printHeaderMandatoryColumns(String... columns) {
        // Do nothing
    }

    @Override
    public void printHeaderRemainingColumns(Collection<PropertyTypeInfo> remainingColumns) {
        // Do nothing
    }

    @Override
    public void printProperties(Map<?, ?> properties) throws IOException {
        throw new RuntimeException("Neptune Streams JSON is not support for this command");
    }

    @Override
    public void printProperties(String id, String type, Map<?, ?> properties) throws IOException {
        for (Map.Entry<Object, PropertyTypeInfo> entry : metadata.entrySet()) {

            Object key = entry.getKey();
            PropertyTypeInfo propertyTypeInfo = entry.getValue();

            DataType dataType = propertyTypeInfo.dataType();
            String formattedKey = propertyTypeInfo.nameWithoutDataType();

            if (properties.containsKey(key)) {

                Object value = properties.get(key);

                if (isList(value)) {

                    List<?> values = (List<?>) value;
                    for (Object o : values) {
                        printRecord(id, type, formattedKey, o, dataType);
                    }

                } else {
                    printRecord(id, type, formattedKey, value, dataType);
                }
            }
        }
    }

    @Override
    public void printEdge(String id, String label, String from, String to) throws IOException {
        printRecord(id, "e", "label", label, DataType.String, from, to);
    }

    @Override
    public void printNode(String id, String label) throws IOException {
        printRecord(id, "vl", "label", label, DataType.String);
    }

    @Override
    public void printStartRow() throws IOException {
        writer.start();
    }

    @Override
    public void printEndRow() throws IOException {
        generator.flush();
        writer.finish();
    }

    @Override
    public void close() throws Exception {
        generator.close();
    }

    private void printRecord(String id, String type, String key, Object value, DataType dataType) throws IOException {
        printRecord(id, type, key, value, dataType, null, null);
    }

    private void printRecord(String id, String type, String key, Object value, DataType dataType, String from, String to) throws IOException {

        generator.writeStartObject();

        generator.writeObjectFieldStart("eventId");
        generator.writeNumberField("commitNum", -1);
        generator.writeNumberField("opNum", 0);
        generator.writeEndObject();

        generator.writeObjectFieldStart("data");
        generator.writeStringField("id", id);
        generator.writeStringField("type", type);
        generator.writeStringField("key", key);

        generator.writeObjectFieldStart("value");
        dataType.printTo(generator, "value", value);
        generator.writeStringField("dataType", dataType.name());
        generator.writeEndObject();

        if (from != null) {
            generator.writeStringField("from", from);
        }
        if (to != null) {
            generator.writeStringField("to", to);
        }

        generator.writeEndObject();

        generator.writeStringField("op", "ADD");
        generator.writeEndObject();

    }

    private boolean isList(Object value) {
        return value.getClass().isAssignableFrom(java.util.ArrayList.class);
    }
}

