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

import java.util.*;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVRecord;

import java.util.stream.Collectors;

public class VertexMetadata {

    public static VertexMetadata parse(CSVRecord record, PropertyValueParser parser, ConversionConfig conversionConfig) {

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
        return new VertexMetadata(headers, lastColumnIndex, parser, conversionConfig);
    }

    private final Headers headers;
    private final int lastColumnIndex;
    private final PropertyValueParser propertyValueParser;
    private final ConversionConfig conversionConfig;
    private final Set<String> skippedVertexIds;

    private VertexMetadata(Headers headers, int lastColumnIndex, PropertyValueParser parser, ConversionConfig conversionConfig) {
        this.headers = headers;
        this.lastColumnIndex = lastColumnIndex;
        this.propertyValueParser = parser;
        this.conversionConfig = conversionConfig == null ? new ConversionConfig() : conversionConfig;
        this.skippedVertexIds = new HashSet<>();
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

    public Optional<Iterable<String>> toIterable(CSVRecord record) {
        if (shouldSkipVertex(record)) {
            return Optional.empty();
        }
        return Optional.of(() -> new Iterator<String>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index <= lastColumnIndex;
            }

            @Override
            public String next() {
                Header header = headers.get(index);

                if (header.equals(Token.LABEL)) {
                    String originalLabels = record.get(index++);
                    return mapVertexLabels(originalLabels);
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
        });
    }

    String mapVertexLabels(String originalLabels) {
        if (originalLabels == null || originalLabels.trim().isEmpty()) {
            return originalLabels;
        }

        return Arrays.stream(originalLabels.split(":"))
                .filter(s -> !s.isEmpty())
                .map(label -> conversionConfig.getVertexLabels().getOrDefault(label.trim(), label.trim()))
                .collect(Collectors.joining(";"));
    }

    boolean shouldSkipVertex(CSVRecord record) {
        Set<String> skipVertexIds = conversionConfig.getSkipVertices().getById();
        Set<String> skipVertexLabels = conversionConfig.getSkipVertices().getByLabel();
        if (CollectionUtils.isEmpty(skipVertexLabels) && CollectionUtils.isEmpty(skipVertexIds)) {
            return false;
        }

        String vertexId = record.get(0); // _id is always the first column

        // Check if vertex ID should be skipped
        if (!skipVertexIds.isEmpty() && skipVertexIds.contains(vertexId)) {
            skippedVertexIds.add(vertexId);
            return true;
        }

        // Only retrieve and check labels if we have label-based skip rules
        if (!skipVertexLabels.isEmpty()) {
            String vertexLabels = getVertexLabels(record);

            if (vertexLabels != null && !vertexLabels.isEmpty()) {
                String[] labels = vertexLabels.split(":");
                for (String label : labels) {
                    String trimmedLabel = label.trim();
                    if (!trimmedLabel.isEmpty() && skipVertexLabels.contains(trimmedLabel)) {
                        skippedVertexIds.add(vertexId);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private String getVertexLabels(CSVRecord record) {
        // In Neo4j CSV exports, _labels is typically at index 1 (after _id at index 0)
        if (record.size() > 1) {
            return record.get(1);
        }
        return null;
    }

    /**
     * Get the set of skipped vertex IDs for edge filtering
     */
    public Set<String> getSkippedVertexIds() {
        return Collections.unmodifiableSet(skippedVertexIds);
    }
}
