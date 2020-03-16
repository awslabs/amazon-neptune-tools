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
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class NeptuneStreamsJsonPropertyGraphPrinter implements PropertyGraphPrinter {

    private final static AtomicLong COMMIT_NUM_GENERATOR = new AtomicLong(1);

    private final OutputWriter writer;
    private final JsonGenerator generator;

    private long commitNum = 1;
    private int opNum = 1;

    public NeptuneStreamsJsonPropertyGraphPrinter(OutputWriter writer, JsonGenerator generator) throws IOException {
        this.writer = writer;
        this.generator = generator;
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
        throw new RuntimeException("Neptune Streams JSON is not supported for this command");
    }

    @Override
    public void printProperties(String id, String streamOperation, Map<?, ?> properties) throws IOException {

        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object value = entry.getValue();

            PropertyTypeInfo propertyTypeInfo = new PropertyTypeInfo(key);
            propertyTypeInfo.accept(value);

            if (isList(value)) {

                List<?> values = (List<?>) value;
                for (Object o : values) {
                    printRecord(id, streamOperation, key, o, propertyTypeInfo.dataType());
                }

            } else {
                printRecord(id, streamOperation, key, value, propertyTypeInfo.dataType());
            }
        }
    }

    @Override
    public void printEdge(String id, String label, String from, String to) throws IOException {
        printRecord(id, "e", "label", label, DataType.String, from, to);
    }

    @Override
    public void printNode(String id, List<String> labels) throws IOException {
        for (String l : labels) {
            printRecord(id, "vl", "label", l, DataType.String);
        }
    }

    @Override
    public void printStartRow() throws IOException {
        commitNum = COMMIT_NUM_GENERATOR.getAndIncrement();
        opNum = 1;

        writer.startCommit();
    }

    @Override
    public void printEndRow() throws IOException {
        generator.flush();
        writer.endCommit();
    }

    @Override
    public void close() throws Exception {
        generator.close();
        writer.close();
    }

    private void printRecord(String id, String streamOperation, String key, Object value, DataType dataType) throws IOException {
        printRecord(id, streamOperation, key, value, dataType, null, null);
    }

    private void printRecord(String id, String streamOperation, String key, Object value, DataType dataType, String from, String to) throws IOException {

        writer.startOp();
        generator.writeStartObject();

        generator.writeObjectFieldStart("eventId");
        generator.writeNumberField("commitNum", commitNum);
        generator.writeNumberField("opNum", opNum++);
        generator.writeEndObject();

        generator.writeObjectFieldStart("data");
        generator.writeStringField("id", id);
        generator.writeStringField("type", streamOperation);
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
        generator.flush();
        writer.endOp();
    }

    private boolean isList(Object value) {
        return value.getClass().isAssignableFrom(java.util.ArrayList.class);
    }
}

