/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import org.apache.commons.csv.CSVRecord;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class VertexMetadata {

    public static VertexMetadata parse(CSVRecord record, PropertyValueParser parser) {

        Headers headers = new Headers();

        int lastColumnIndex = -1;

        for (String header : record) {
            if (header.equalsIgnoreCase("_start")) {
                break;
            } else {
                lastColumnIndex++;
            }

            switch (header) {
                case "_id":
                    headers.add(Token.ID);
                    break;
                case "_labels":
                    headers.add(Token.LABEL);
                    break;
                default:
                    headers.add(new Property(header));
            }
        }
        return new VertexMetadata(headers, lastColumnIndex, parser);
    }

    private final Headers headers;
    private final int lastColumnIndex;
    private final PropertyValueParser propertyValueParser;

    private VertexMetadata(Headers headers, int lastColumnIndex, PropertyValueParser parser) {
        this.headers = headers;
        this.lastColumnIndex = lastColumnIndex;
        this.propertyValueParser = parser;
    }

    public List<String> headers() {
        return headers.values();
    }

    int lastColumnIndex() {
        return lastColumnIndex;
    }

    public boolean isVertex(CSVRecord record) {
        return !record.get(0).isEmpty();
    }

    public Iterable<String> toIterable(CSVRecord record) {
        return () -> new Iterator<String>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index <= lastColumnIndex;
            }

            @Override
            public String next() {
                Header header = headers.get(index);

                if (header.equals(Token.LABEL)) {
                    return Arrays.stream(record.get(index++).split(":"))
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.joining(";"));
                } else {
                    PropertyValue propertyValue = propertyValueParser.parse(record.get(index));
                    if (propertyValue.isMultiValued()) {
                        header.setIsMultiValued(true);
                    }
                    header.updateDataType(propertyValue.dataType());
                    index++;
                    return propertyValue.value();
                }
            }
        };
    }
}
