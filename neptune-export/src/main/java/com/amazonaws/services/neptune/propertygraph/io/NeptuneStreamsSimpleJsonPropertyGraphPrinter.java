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
import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class NeptuneStreamsSimpleJsonPropertyGraphPrinter implements PropertyGraphPrinter {

//    private static final AtomicLong COMMIT_NUM_GENERATOR = new AtomicLong(1);

    private final OutputWriter writer;
    private final JsonGenerator generator;

//    private long commitNum = 1;
//    private int opNum = 1;

    public NeptuneStreamsSimpleJsonPropertyGraphPrinter(OutputWriter writer,
                                                        JsonGenerator generator) throws IOException {
        this.writer = writer;
        this.generator = generator;
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
        throw new RuntimeException("Neptune Streams simple JSON is not supported for this command");
    }

    @Override
    public void printProperties(Map<?, ?> properties, boolean applyFormatting) throws IOException {
        printProperties(properties);
    }

    @Override
    public void printProperties(String id, String streamOperation, Map<?, ?> properties) throws IOException {

        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object value = entry.getValue();

            if (isList(value)) {

                List<?> values = (List<?>) value;
                for (Object o : values) {
                    PropertySchema propertySchema = new PropertySchema(key);
                    propertySchema.accept(o, true);
                    printRecord(id, streamOperation, key, o, propertySchema.dataType());
                }

            } else {
                PropertySchema propertySchema = new PropertySchema(key);
                propertySchema.accept(value, true);
                printRecord(id, streamOperation, key, value, propertySchema.dataType());
            }
        }
    }

    @Override
    public void printEdge(String id, String label, String from, String to) throws IOException {
        printEdge(id, label, from, to, null, null);
    }

    @Override
    public void printEdge(String id, String label, String from, String to, Collection<String> fromLabels, Collection<String> toLabels) throws IOException {
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

//        commitNum = COMMIT_NUM_GENERATOR.getAndIncrement();
//        opNum = 1;

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

        //generator.writeNumberField("commitNum", commitNum);
        //generator.writeNumberField("opNum", opNum++);

        generator.writeStringField("id", id);

        if (from != null) {
            generator.writeStringField("from", from);
        } else {
            generator.writeStringField("from", "");
        }
        if (to != null) {
            generator.writeStringField("to", to);
        } else {
            generator.writeStringField("to", "");
        }

        generator.writeStringField("type", streamOperation);
        generator.writeStringField("key", key);
        dataType.printAsStringTo(generator, "value", value);
        generator.writeStringField("dataType", dataType.name());

        generator.writeStringField("s", "");
        generator.writeStringField("p", "");
        generator.writeStringField("o", "");
        generator.writeStringField("g", "");
        generator.writeStringField("stmt", "");


        //generator.writeStringField("op", "ADD");

        generator.writeEndObject();
        generator.writeRaw(writer.lineSeparator());
        generator.flush();
        writer.endOp();
    }

    private boolean isList(Object value) {
        return value instanceof List<?>;
    }
}

