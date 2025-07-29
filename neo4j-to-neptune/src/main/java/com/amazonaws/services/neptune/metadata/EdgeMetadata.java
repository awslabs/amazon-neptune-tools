/*
Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    private static final String EDGE_ID_KEY = "~id";

    public static EdgeMetadata parse(CSVRecord record, PropertyValueParser parser,
            ConversionConfig conversionConfig, Set<String> skippedVertexIds, Map<String, String> vertexIdMap) {
        return parse(record, ID_GENERATOR, parser, conversionConfig, skippedVertexIds, vertexIdMap);
    }

    public static EdgeMetadata parse(CSVRecord record, Supplier<String> idGenerator, PropertyValueParser parser,
            ConversionConfig conversionConfig, Set<String> skippedVertexIds, Map<String, String> vertexIdMap) {

        Headers headers = new Headers();
        headers.add(Token.GREMLIN_ID);

        boolean isEdgeHeader = false;
        int firstColumnIndex = 0;

        for (String header : record) {
            if (header.equalsIgnoreCase("_start")) {
                isEdgeHeader = true;
            }
            if (isEdgeHeader) {
                switch (header) {
                    case "_start":
                        headers.add(Token.GREMLIN_FROM);
                        break;
                    case "_end":
                        headers.add(Token.GREMLIN_TO);
                        break;
                    case "_type":
                        headers.add(Token.GREMLIN_LABEL);
                        break;
                    default:
                        headers.add(new Property(header));
                }
            } else {
                firstColumnIndex++;
            }
        }
        return new EdgeMetadata(headers, firstColumnIndex, idGenerator, parser, conversionConfig, skippedVertexIds, vertexIdMap);
    }

    private final Headers headers;
    private final int firstColumnIndex;
    private final Supplier<String> idGenerator;
    private final PropertyValueParser propertyValueParser;
    private final ConversionConfig conversionConfig;
    private final Set<String> skippedVertexIds;
    private final Map<String, String> vertexIdMap;

    // Cache for property name to index mapping
    private final Map<String, Integer> propertyIndexMap = new HashMap<>();

    // Cache for edge template
    private final String edgeIdTemplate;

    protected EdgeMetadata(Headers headers,
                         int firstColumnIndex,
                         Supplier<String> idGenerator,
                         PropertyValueParser parser,
                         ConversionConfig conversionConfig,
                         Set<String> skippedVertexIds) {
        this(headers, firstColumnIndex, idGenerator, parser, conversionConfig, skippedVertexIds, Collections.emptyMap());
    }

    protected EdgeMetadata(Headers headers,
                         int firstColumnIndex,
                         Supplier<String> idGenerator,
                         PropertyValueParser parser,
                         ConversionConfig conversionConfig,
                         Set<String> skippedVertexIds,
                         Map<String, String> vertexIdMap) {
        this.headers = headers;
        this.firstColumnIndex = firstColumnIndex;
        this.idGenerator = idGenerator;
        this.propertyValueParser = parser;
        this.conversionConfig = conversionConfig != null ? conversionConfig : new ConversionConfig();
        this.skippedVertexIds = skippedVertexIds != null ? skippedVertexIds : Collections.emptySet();
        this.vertexIdMap = vertexIdMap != null ? vertexIdMap : Collections.emptyMap();

        // Cache the edge ID template
        this.edgeIdTemplate = this.conversionConfig.getEdgeIdTransformation().get(EDGE_ID_KEY);

        // Pre-compute property name to index mapping for faster lookups
        for (int i = 1; i < headers.count(); i++) {
            Header header = headers.get(i);
            if (header instanceof Property) {
                int recordIndex = firstColumnIndex + i - 1;
                propertyIndexMap.put(((Property) header).getName(), recordIndex);
            }
        }
    }

    public List<String> headers() {
        return headers.values();
    }

    public int firstColumnIndex() {
        return firstColumnIndex;
    }

    public boolean isEdge(CSVRecord record) {
        return record.size() > firstColumnIndex + 2 &&
            !record.get(firstColumnIndex).isEmpty() &&
            !record.get(firstColumnIndex + 1).isEmpty() &&
            !record.get(firstColumnIndex + 2).isEmpty();
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
                    return mapEdgeId(idGenerator.get(), record);
                } else {
                    int headerIndex = currentColumnIndex - firstColumnIndex + 1;
                    Header header = headers.get(headerIndex);
                    String value = record.get(currentColumnIndex++);

                    if (header.equals(Token.NEO4J_LABELS)) {
                        return value;
                    } else if (header.equals(Token.GREMLIN_FROM) || header.equals(Token.NEO4J_START)) {
                        return transformVertexId(value);
                    } else if (header.equals(Token.GREMLIN_TO) || header.equals(Token.NEO4J_END)) {
                        return transformVertexId(value);
                    } else if (header.equals(Token.GREMLIN_LABEL) || header.equals(Token.NEO4J_TYPE)) {
                        return mapEdgeLabel(value);
                    } else {
                        PropertyValue propertyValue = propertyValueParser.parse(value);
                        header.updateDataType(propertyValue.dataType());
                        return propertyValue.value();
                    }
                }
            }
        });
    }

    String mapEdgeId(String originalId, CSVRecord record) {
        if (edgeIdTemplate == null || edgeIdTemplate.isEmpty()) {
            return originalId;
        }

        // Quick check if template has no placeholders
        if (!edgeIdTemplate.contains("{")) {
            return edgeIdTemplate;
        }

        // Start with the template and replace {_id}
        String result = edgeIdTemplate.replace(Token.valueWithCurlyBraces(Token.NEO4J_ID), originalId);

        // Support for {_type} placeholder (Neo4j format)
        if (result.contains(Token.valueWithCurlyBraces(Token.NEO4J_TYPE))) {
            String edgeType = record.get(firstColumnIndex + 2);
            result = result.replace(Token.valueWithCurlyBraces(Token.NEO4J_TYPE), mapEdgeLabel(edgeType));
        }

        // Support for {_start} placeholder (Neo4j format)
        if (result.contains(Token.valueWithCurlyBraces(Token.NEO4J_START))) {
            String fromId = record.get(firstColumnIndex);
            String transformedFromId = transformVertexId(fromId);
            result = result.replace(Token.valueWithCurlyBraces(Token.NEO4J_START), transformedFromId);
        }

        // Support for {_end} placeholder (Neo4j format)
        if (result.contains(Token.valueWithCurlyBraces(Token.NEO4J_END))) {
            String toId = record.get(firstColumnIndex + 1);
            String transformedToId = transformVertexId(toId);
            result = result.replace(Token.valueWithCurlyBraces(Token.NEO4J_END), transformedToId);
        }

        // Support for {~label} if present (Gremlin format)
        if (result.contains(Token.valueWithCurlyBraces(Token.GREMLIN_LABEL))) {
            String edgeType = record.get(firstColumnIndex + 2);
            result = result.replace(Token.valueWithCurlyBraces(Token.GREMLIN_LABEL), mapEdgeLabel(edgeType));
        }

        // Support for {~from} placeholder (Gremlin format)
        if (result.contains(Token.valueWithCurlyBraces(Token.GREMLIN_FROM))) {
            String fromId = record.get(firstColumnIndex);
            String transformedFromId = transformVertexId(fromId);
            result = result.replace(Token.valueWithCurlyBraces(Token.GREMLIN_FROM), transformedFromId);
        }

        // Support for {~to} placeholder (Gremlin format)
        if (result.contains(Token.valueWithCurlyBraces(Token.GREMLIN_TO))) {
            String toId = record.get(firstColumnIndex + 1);
            String transformedToId = transformVertexId(toId);
            result = result.replace(Token.valueWithCurlyBraces(Token.GREMLIN_TO), transformedToId);
        }

        // Replace property placeholders using the cached property index map
        for (Map.Entry<String, Integer> entry : propertyIndexMap.entrySet()) {
            String propName = entry.getKey();
            String placeholder = "{" + propName + "}";

            if (result.contains(placeholder)) {
                int index = entry.getValue();
                if (index < record.size()) {
                    result = result.replace(placeholder, record.get(index));
                }
            }
        }

        // Check if any placeholders remain
        int placeholderStart = result.indexOf('{');
        if (placeholderStart >= 0) {
            int placeholderEnd = result.indexOf('}', placeholderStart);
            if (placeholderEnd > placeholderStart) {
                String placeholder = result.substring(placeholderStart + 1, placeholderEnd);
                throw new IllegalArgumentException("Property {" + placeholder + "} not found in CSV headers");
            }
        }

        return result;
    }

    public String mapEdgeLabel(String originalLabel) {
        if (originalLabel == null || originalLabel.trim().isEmpty()) {
            return originalLabel;
        }

        return conversionConfig.getEdgeLabels().getOrDefault(originalLabel.trim(), originalLabel.trim());
    }

    boolean shouldSkipEdge(CSVRecord record) {
        // An edge might still be skipped if its connected vertex is skipped
        if (conversionConfig == null || !conversionConfig.hasSkipRules() && skippedVertexIds.isEmpty()) {
            return false;
        }

        Set<String> skipEdgeLabels = conversionConfig.getSkipEdges().getByLabel();

        // Make sure we have enough columns for edge data
        if (record.size() <= firstColumnIndex + 2) {
            return false; // Not enough data to be a valid edge
        }

        // The actual edge data starts at firstColumnIndex
        String startVertexId = record.get(firstColumnIndex);     // _start
        String endVertexId = record.get(firstColumnIndex + 1);   // _end
        String edgeType = record.get(firstColumnIndex + 2);      // _type

        // Skip edge if either connected vertex was skipped, note this does rely on nodes being processed first
        if (skippedVertexIds.contains(startVertexId) || skippedVertexIds.contains(endVertexId)) {
            return true;
        }

        // Check if edge type/label should be skipped
        if (edgeType != null && !edgeType.trim().isEmpty() && !skipEdgeLabels.isEmpty() && skipEdgeLabels.contains(edgeType.trim())) {
            return true;
        }

        return false;
    }

    /**
     * Transform a vertex ID using the vertex ID mapping.
     * This method can be overridden by subclasses to provide custom transformation logic.
     *
     * @param originalId The original vertex ID
     * @return The transformed vertex ID, or the original ID if no transformation is found
     */
    protected String transformVertexId(String originalId) {
        return vertexIdMap.getOrDefault(originalId, originalId);
    }
}
