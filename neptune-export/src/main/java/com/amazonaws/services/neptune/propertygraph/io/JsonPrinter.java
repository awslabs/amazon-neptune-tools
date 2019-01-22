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

public class JsonPrinter implements Printer {

    private final JsonGenerator generator;
    private final Map<String, PropertyTypeInfo> metadata;

    public JsonPrinter(JsonGenerator generator, Map<String, PropertyTypeInfo> metadata) throws IOException {
        this.generator = generator;
        this.metadata = metadata;
    }

    @Override
    public void printHeaderMandatoryColumns(String... columns) {
        // Do nothing
    }

    @Override
    public void printHeaderRemainingColumns(Collection<PropertyTypeInfo> remainingColumns, boolean includeTypeDefinitions) {
        // Do nothing
    }

    @Override
    public void printProperties(Map<?, ?> properties) throws IOException {
        for (Map.Entry<String, PropertyTypeInfo> entry : metadata.entrySet()) {

            String key = entry.getKey();
            DataType dataType = entry.getValue().dataType();
            if (properties.containsKey(key)) {

                Object value = properties.get(key);

                if (isList(value)) {
                    List<?> values = (List<?>) value;
                    if (values.size() > 1) {
                        generator.writeFieldName(key);
                        generator.writeStartArray();
                        for (Object v : values) {
                            dataType.printTo(generator, v);
                        }
                        generator.writeEndArray();
                    } else {
                        dataType.printTo(generator, key, values.get(0));
                    }

                } else {
                    dataType.printTo(generator, key, value);
                }
            }
        }
    }

    @Override
    public void printEdge(String id, String label, String from, String to) throws IOException {
        generator.writeStringField("~id", id);
        generator.writeStringField("~label", label);
        generator.writeStringField("~from", from);
        generator.writeStringField("~to", to);
    }

    @Override
    public void printNode(String id, String label) throws IOException {
        generator.writeStringField("~id", id);
        generator.writeStringField("~label", label);
    }

    @Override
    public void printStartRow() throws IOException {
        generator.writeStartObject();
    }

    @Override
    public void printEndRow() throws IOException {
        generator.writeEndObject();
    }

    @Override
    public void close() throws Exception {
        generator.close();
    }

    private boolean isList(Object value) {
        return value.getClass().isAssignableFrom(java.util.ArrayList.class);
    }

}
