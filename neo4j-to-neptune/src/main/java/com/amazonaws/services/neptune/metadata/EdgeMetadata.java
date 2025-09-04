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

    public static EdgeMetadata parse(CSVRecord csvRecord, PropertyValueParser parser,
            ConversionConfig conversionConfig, Set<String> skippedVertexIds, Map<String, String> vertexIdMap) {
        return parse(csvRecord, ID_GENERATOR, parser, conversionConfig, skippedVertexIds, vertexIdMap);
    }

    public static EdgeMetadata parse(CSVRecord csvRecord, Supplier<String> idGenerator, PropertyValueParser parser,
            ConversionConfig conversionConfig, Set<String> skippedVertexIds, Map<String, String> vertexIdMap) {

        Headers headers = new Headers();
        headers.add(Token.GREMLIN_ID);

        boolean isEdgeHeader = false;
        int firstColumnIndex = 0;

        for (String header : csvRecord) {
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
            if (header instanceof Property property) {
                int recordIndex = firstColumnIndex + i - 1;
                propertyIndexMap.put(property.getName(), recordIndex);
            }
        }
    }

    public List<String> headers() {
        return headers.values();
    }

    public int firstColumnIndex() {
        return firstColumnIndex;
    }

    public boolean isEdge(CSVRecord csvRecord) {
        return csvRecord.size() > firstColumnIndex + 2 &&
            !csvRecord.get(firstColumnIndex).isEmpty() &&
            !csvRecord.get(firstColumnIndex + 1).isEmpty() &&
            !csvRecord.get(firstColumnIndex + 2).isEmpty();
    }

    public Optional<Iterable<String>> toIterable(CSVRecord csvRecord) {
        if (shouldSkipEdge(csvRecord)) {
            return Optional.empty();
        }
        return Optional.of(() -> new EdgeIterator(csvRecord));
    }

    boolean shouldSkipEdge(CSVRecord csvRecord) {
        if (hasNoSkipRules()) {
            return false;
        }
        if (hasInsufficientHeaderColumns(csvRecord)) {
            return false;
        }
        return shouldSkipBasedOnVertices(csvRecord) || shouldSkipBasedOnLabel(csvRecord);
    }

    private boolean hasNoSkipRules() {
        return conversionConfig == null || (!conversionConfig.hasSkipRules() && skippedVertexIds.isEmpty());
    }

    private boolean hasInsufficientHeaderColumns(CSVRecord csvRecord) {
        return csvRecord.size() <= firstColumnIndex + 2;
    }

    private boolean shouldSkipBasedOnVertices(CSVRecord csvRecord) {
        String startVertexId = csvRecord.get(firstColumnIndex);
        String endVertexId = csvRecord.get(firstColumnIndex + 1);
        return skippedVertexIds.contains(startVertexId) || skippedVertexIds.contains(endVertexId);
    }

    private boolean shouldSkipBasedOnLabel(CSVRecord csvRecord) {
        Set<String> skipEdgeLabels = conversionConfig.getSkipEdges().getByLabel();
        String edgeType = csvRecord.get(firstColumnIndex + 2);

        // Check if edge type/label should be skipped
        return edgeType != null &&
            !edgeType.trim().isEmpty() &&
            !skipEdgeLabels.isEmpty() &&
            skipEdgeLabels.contains(edgeType.trim());
    }

    private class EdgeIterator implements Iterator<String> {
        private final CSVRecord csvRecord;
        private int currentColumnIndex;

        EdgeIterator(CSVRecord csvRecord) {
            this.csvRecord = csvRecord;
            this.currentColumnIndex = firstColumnIndex - 1;
        }

        @Override
        public boolean hasNext() {
            return currentColumnIndex < csvRecord.size();
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (currentColumnIndex < firstColumnIndex) {
                currentColumnIndex++;
                return mapEdgeId(idGenerator.get(), csvRecord);
            }
            return processHeaderValue();
        }

        private String processHeaderValue() {
            int headerIndex = currentColumnIndex - firstColumnIndex + 1;
            Header header = headers.get(headerIndex);
            String value = csvRecord.get(currentColumnIndex++);
            return transformValue(header, value);
        }

        private String transformValue(Header header, String value) {
            if (header.equals(Token.NEO4J_LABELS)) {
                return value;
            }
            if (isVertexHeader(header)) {
                return transformVertexId(value);
            }
            if (isLabelHeader(header)) {
                return mapEdgeLabel(value);
            }
            return processProperty(header, value);
        }

        private boolean isVertexHeader(Header header) {
            return header.equals(Token.GREMLIN_FROM) || header.equals(Token.NEO4J_START) ||
                   header.equals(Token.GREMLIN_TO) || header.equals(Token.NEO4J_END);
        }

        private boolean isLabelHeader(Header header) {
            return header.equals(Token.GREMLIN_LABEL) || header.equals(Token.NEO4J_TYPE);
        }

        private String processProperty(Header header, String value) {
            PropertyValue propertyValue = propertyValueParser.parse(value);
            header.updateDataType(propertyValue.dataType());
            return propertyValue.value();
        }
    }

    String mapEdgeId(String originalId, CSVRecord csvRecord) {
        if (edgeIdTemplate == null || edgeIdTemplate.isEmpty()) {
            return originalId;
        }
        if (!edgeIdTemplate.contains("{")) {
            return edgeIdTemplate;
        }
        String result = edgeIdTemplate.replace(Token.valueWithCurlyBraces(Token.NEO4J_ID), originalId);
        result = replaceHeaderPlaceholders(result, csvRecord);
        result = replacePropertyPlaceholders(result, csvRecord);
        validateNoRemainingPlaceholders(result);
        return result;
    }

    private String replaceHeaderPlaceholders(String template, CSVRecord csvRecord) {
        String result = template;
        result = replaceEdgeTypePlaceholders(result, csvRecord);
        result = replaceVertexPlaceholders(result, csvRecord);
        return result;
    }

    private String replaceEdgeTypePlaceholders(String template, CSVRecord csvRecord) {
        String edgeType = csvRecord.get(firstColumnIndex + 2);
        String mappedLabel = mapEdgeLabel(edgeType);
        String result = template;
        result = replacePlaceholder(result, Token.NEO4J_TYPE, mappedLabel);
        result = replacePlaceholder(result, Token.GREMLIN_LABEL, mappedLabel);
        return result;
    }

    private String replaceVertexPlaceholders(String template, CSVRecord csvRecord) {
        String fromId = transformVertexId(csvRecord.get(firstColumnIndex));
        String toId = transformVertexId(csvRecord.get(firstColumnIndex + 1));
        String result = template;
        result = replacePlaceholder(result, Token.NEO4J_START, fromId);
        result = replacePlaceholder(result, Token.NEO4J_END, toId);
        result = replacePlaceholder(result, Token.GREMLIN_FROM, fromId);
        result = replacePlaceholder(result, Token.GREMLIN_TO, toId);
        return result;
    }

    private String replacePlaceholder(String template, Token token, String value) {
        String placeholder = Token.valueWithCurlyBraces(token);
        return template.contains(placeholder) ? template.replace(placeholder, value) : template;
    }

    private String replacePropertyPlaceholders(String template, CSVRecord csvRecord) {
        String result = template;
        for (Map.Entry<String, Integer> entry : propertyIndexMap.entrySet()) {
            result = replacePropertyPlaceholder(result, entry, csvRecord);
        }
        return result;
    }

    private String replacePropertyPlaceholder(String template, Map.Entry<String, Integer> entry, CSVRecord csvRecord) {
        String propName = entry.getKey();
        String placeholder = "{" + propName + "}";
        if (!template.contains(placeholder)) {
            return template;
        }
        int index = entry.getValue();
        if (index >= csvRecord.size()) {
            return template;
        }
        return template.replace(placeholder, csvRecord.get(index));
    }

    private void validateNoRemainingPlaceholders(String result) {
        int placeholderStart = result.indexOf('{');
        if (placeholderStart < 0) {
            return;
        }
        int placeholderEnd = result.indexOf('}', placeholderStart);
        if (placeholderEnd <= placeholderStart) {
            return;
        }
        String placeholder = result.substring(placeholderStart + 1, placeholderEnd);
        throw new IllegalArgumentException("Property {" + placeholder + "} not found in CSV headers");
    }

    public String mapEdgeLabel(String originalLabel) {
        if (originalLabel == null || originalLabel.trim().isEmpty()) {
            return originalLabel;
        }

        return conversionConfig.getEdgeLabels().getOrDefault(originalLabel.trim(), originalLabel.trim());
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
