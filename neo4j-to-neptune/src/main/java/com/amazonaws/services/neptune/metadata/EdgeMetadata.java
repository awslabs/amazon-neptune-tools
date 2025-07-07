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

import java.util.*;
import java.util.function.Supplier;

public class EdgeMetadata {

    private static final Supplier<String> ID_GENERATOR = () -> UUID.randomUUID().toString();

    static EdgeMetadata parse(CSVRecord record, Supplier<String> idGenerator, PropertyValueParser parser,
                              ConversionConfig conversionConfig, Set<String> skippedVertexIds) {

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
        return new EdgeMetadata(headers, firstColumnIndex, idGenerator, parser, conversionConfig, skippedVertexIds);
    }

    public static EdgeMetadata parse(CSVRecord record, PropertyValueParser parser,
                                     ConversionConfig conversionConfig, Set<String> skippedVertexIds) {
        return parse(record, ID_GENERATOR, parser, conversionConfig, skippedVertexIds);
    }

    private final Headers headers;
    private final int firstColumnIndex;
    private final Supplier<String> idGenerator;
    private final PropertyValueParser propertyValueParser;
    private final ConversionConfig conversionConfig;
    private final Set<String> skippedVertexIds;

    private EdgeMetadata(Headers headers,
                         int firstColumnIndex,
                         Supplier<String> idGenerator,
                         PropertyValueParser parser,
                         ConversionConfig conversionConfig,
                         Set<String> skippedVertexIds) {
        this.headers = headers;
        this.firstColumnIndex = firstColumnIndex;
        this.idGenerator = idGenerator;
        this.propertyValueParser = parser;
        this.conversionConfig = conversionConfig;
        this.skippedVertexIds = skippedVertexIds != null ? skippedVertexIds : new HashSet<>();
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

    public Optional<Iterable<String>> toIterable(CSVRecord record) {
        if (shouldSkipEdge(record)) {
            return Optional.empty();
        }
        return Optional.of(() -> new Iterator<String>() {
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

                    String value = record.get(currentColumnIndex);

                    // Apply label mapping for edge labels
                    if (header.equals(Token.LABEL)) {
                        value = mapEdgeLabel(value);
                        currentColumnIndex++;
                        return value;
                    } else {
                        PropertyValue propertyValue = propertyValueParser.parse(value);
                        header.updateDataType(propertyValue.dataType());
                        currentColumnIndex++;
                        return propertyValue.value();
                    }
                }
            }
        });
    }

    /**
     * Maps edge labels. Neo4j edge types are single values.
     *
     * @param originalLabel The original edge type from Neo4j
     * @return The mapped edge type, or original if no mapping exists
     */
    String mapEdgeLabel(String originalLabel) {
        if (originalLabel == null || originalLabel.trim().isEmpty()) {
            return originalLabel;
        }

        return conversionConfig.getEdgeLabels().getOrDefault(originalLabel.trim(), originalLabel.trim());
    }

    /**
     * Determines if an edge should be skipped based on its ID, label, or connected vertices.
     *
     * @param record The CSV record representing the edge
     * @return true if the edge should be skipped, false otherwise
     */
    boolean shouldSkipEdge(CSVRecord record) {
        // An edge might still be skipped if its connected vertex is skipped
        Set<String> skipEdgeLabels = conversionConfig.getSkipEdges().getByLabel();
        if (!conversionConfig.hasSkipRules()) {
            return false;
        }

        // Make sure we have enough columns for edge data
        if (record.size() <= firstColumnIndex + 2) {
            return false; // Not enough data to be a valid edge
        }

        // The actual edge data starts at firstColumnIndex
        String startVertexId = record.get(firstColumnIndex);     // _start
        String endVertexId = record.get(firstColumnIndex + 1);   // _end
        String edgeType = record.get(firstColumnIndex + 2);      // _type

        // Skip edge if either connected vertex was skipped, note this does rely on nodes being processed first
        if (!skippedVertexIds.isEmpty() &&
                (skippedVertexIds.contains(startVertexId) || skippedVertexIds.contains(endVertexId)))  {
            return true;
        }

        // Check if edge type/label should be skipped
        if (edgeType != null && !edgeType.trim().isEmpty() && skipEdgeLabels.contains(edgeType.trim())) {
            return true;
        }

        // If needed, this could be extended to support edge property-based filtering.

        return false;
    }

}
