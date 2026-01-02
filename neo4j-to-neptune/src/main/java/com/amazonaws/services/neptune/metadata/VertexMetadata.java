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

import java.util.*;
import org.apache.commons.csv.CSVRecord;
import java.util.stream.Collectors;

public class VertexMetadata {

    private static final String VERTEX_ID_KEY = "~id";

    public static VertexMetadata parse(CSVRecord record, PropertyValueParser parser, ConversionConfig conversionConfig) {
        Headers headers = new Headers();
        int lastColumnIndex = -1;

        for (String header : record) {
            if (header.equalsIgnoreCase(Token.NEO4J_START.value())) {
                break;
            } else {
                lastColumnIndex++;
            }

            switch (header) {
                case "_id":
                    headers.add(Token.GREMLIN_ID);
                    break;
                case "_labels":
                    headers.add(Token.GREMLIN_LABEL);
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
    private final Set<String> skippedVertexIds = new HashSet<>();
    private final Map<String, String> vertexIdMap = new HashMap<>();

    // Cache for property name to index mapping
    private final Map<String, Integer> propertyIndexMap = new HashMap<>();

    private VertexMetadata(Headers headers, int lastColumnIndex, PropertyValueParser parser, ConversionConfig conversionConfig) {
        this.headers = headers;
        this.lastColumnIndex = lastColumnIndex;
        this.propertyValueParser = parser;
        this.conversionConfig = conversionConfig == null ? new ConversionConfig() : conversionConfig;

        // Pre-compute property name to index mapping for faster lookups
        for (int i = 0; i <= lastColumnIndex; i++) {
            Header header = headers.get(i);
            if (header instanceof Property) {
                propertyIndexMap.put(((Property) header).getName(), i);
            }
        }
    }

    public List<String> headers() {
        return headers.values();
    }

    public int lastColumnIndex() {
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

                if (header.equals(Token.GREMLIN_ID)) {
                    String originalId = record.get(index++);
                    String transformedId = mapVertexId(originalId, record);

                    // Store the mapping between original and transformed IDs
                    vertexIdMap.put(originalId, transformedId);

                    return transformedId;
                } else if (header.equals(Token.GREMLIN_LABEL)) {
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

    private String mapVertexId(String originalId, CSVRecord record) {
        if (originalId == null || originalId.trim().isEmpty()) {
            return originalId;
        }

        // Check if there's a template for vertex ID transformation
        String template = conversionConfig.getVertexIdTransformation().get(VERTEX_ID_KEY);
        if (template == null) {
            return originalId;
        }

        // Quick check if template has any placeholders
        if (!template.contains("{")) {
            return template;
        }

        // Start with the template and replace {_id}
        String result = template.replace(Token.valueWithCurlyBraces(Token.NEO4J_ID), originalId);

        // Support for {~id} placeholder (Gremlin format)
        if (result.contains(Token.valueWithCurlyBraces(Token.GREMLIN_ID))) {
            result = result.replace(Token.valueWithCurlyBraces(Token.GREMLIN_ID), originalId);
        }

        // Replace {_labels} if present
        if (result.contains(Token.valueWithCurlyBraces(Token.NEO4J_LABELS))) {
            String rawLabels = record.get(1);
            String mappedLabels = mapVertexLabels(rawLabels).replace(";", "_");
            result = result.replace(Token.valueWithCurlyBraces(Token.NEO4J_LABELS), mappedLabels);
        }

        // Support for {~label} placeholder (Gremlin format)
        if (result.contains(Token.valueWithCurlyBraces(Token.GREMLIN_LABEL))) {
            String rawLabels = record.get(1);
            String mappedLabels = mapVertexLabels(rawLabels).replace(";", "_");
            result = result.replace(Token.valueWithCurlyBraces(Token.GREMLIN_LABEL), mappedLabels);
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
        // Quick check if no skip rules
        if (!conversionConfig.hasSkipRules()) {
            return false;
        }

        String vertexId = record.get(0); // _id is always the first column
        Set<String> skipVertexIds = conversionConfig.getSkipVertices().getById();

        // Check if vertex ID should be skipped
        if (!skipVertexIds.isEmpty() && skipVertexIds.contains(vertexId)) {
            skippedVertexIds.add(vertexId);
            return true;
        }

        // Check if vertex should be skipped by label
        Set<String> skipVertexLabels = conversionConfig.getSkipVertices().getByLabel();
        if (!skipVertexLabels.isEmpty() && record.size() > 1) {
            String vertexLabels = record.get(1);
            if (vertexLabels != null && !vertexLabels.isEmpty()) {
                for (String label : vertexLabels.split(":")) {
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

    /**
     * Get the set of skipped vertex IDs for edge filtering
     */
    public Set<String> getSkippedVertexIds() {
        return Collections.unmodifiableSet(skippedVertexIds);
    }

    /**
     * Get the mapping between original and transformed vertex IDs
     */
    public Map<String, String> getVertexIdMap() {
        return Collections.unmodifiableMap(vertexIdMap);
    }
}
