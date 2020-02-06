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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class EdgeMetadata {

    private static final Supplier<String> ID_GENERATOR = () -> UUID.randomUUID().toString();

    static EdgeMetadata parse(CSVRecord record, Supplier<String> idGenerator, PropertyValueParser parser) {

        Headers headers = new Headers();
        headers.add(Token.ID);

        boolean isEdgeHeader = false;
        int firstColumnIndex = 0;

        for (String header : record) {
            if (header.equalsIgnoreCase("_start")) {
                isEdgeHeader = true;
            }
            if (isEdgeHeader) {
                switch (header) {
                    case "_start":
                        headers.add(Token.FROM);
                        break;
                    case "_end":
                        headers.add(Token.TO);
                        break;
                    case "_type":
                        headers.add(Token.LABEL);
                        break;
                    default:
                        headers.add(new Property(header));
                }
            } else {
                firstColumnIndex++;
            }
        }
        return new EdgeMetadata(headers, firstColumnIndex, idGenerator, parser);
    }

    public static EdgeMetadata parse(CSVRecord record, PropertyValueParser parser) {
        return parse(record, ID_GENERATOR, parser);
    }

    private final Headers headers;
    private final int firstColumnIndex;
    private final Supplier<String> idGenerator;
    private final PropertyValueParser propertyValueParser;

    private EdgeMetadata(Headers headers,
                         int firstColumnIndex,
                         Supplier<String> idGenerator,
                         PropertyValueParser parser) {
        this.headers = headers;
        this.firstColumnIndex = firstColumnIndex;
        this.idGenerator = idGenerator;
        this.propertyValueParser = parser;
    }

    public List<String> headers() {
        return headers.values();
    }

    int firstColumnIndex() {
        return firstColumnIndex;
    }

    public boolean isEdge(CSVRecord record) {
        return (!record.get(firstColumnIndex).isEmpty()) &&
                (!record.get(firstColumnIndex + 1).isEmpty()) &&
                (!record.get(firstColumnIndex + 2).isEmpty());
    }

    public Iterable<String> toIterable(CSVRecord record) {
        return () -> new Iterator<String>() {
            int currentColumnIndex = firstColumnIndex - 1;

            @Override
            public boolean hasNext() {
                return currentColumnIndex < record.size();
            }

            @Override
            public String next() {
                if (currentColumnIndex < firstColumnIndex) {
                    currentColumnIndex++;
                    return idGenerator.get();
                } else {
                    int headerIndex = currentColumnIndex - firstColumnIndex + 1;
                    Header header = headers.get(headerIndex);
                    PropertyValue propertyValue = propertyValueParser.parse(record.get(currentColumnIndex));
                    header.updateDataType(propertyValue.dataType());
                    currentColumnIndex++;
                    return propertyValue.value();
                }
            }
        };
    }
}
