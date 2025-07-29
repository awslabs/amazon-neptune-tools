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

import lombok.Data;
import lombok.NoArgsConstructor;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Configuration class for label mapping and filtering from YAML file.
 * <p>
 * Expected YAML format:
 * vertexLabels:
 *   OldVertexLabel: NewVertexLabel
 *   AnotherOldLabel: AnotherNewLabel
 * edgeLabels:
 *   OLD_EDGE_TYPE: NEW_EDGE_TYPE
 *   ANOTHER_OLD_TYPE: ANOTHER_NEW_TYPE
 * vertexIdTransformation:
 *   ~id: "{_labels}_{_id}"
 * edgeIdTransformation:
 *   ~id: "{_type}_{_start}_{_end}"
 * skipVertices:
 *   byId:
 *     - "vertex_id_1"
 *     - "vertex_id_2"
 *   byLabel:
 *     - "LabelToSkip"
 *     - "AnotherLabelToSkip"
 * skipEdges:
 *   byLabel:
 *     - "RELATIONSHIP_TYPE_TO_SKIP"
 *     - "ANOTHER_TYPE_TO_SKIP"
 */
@Data
@NoArgsConstructor
public class ConversionConfig {

    // Label mapping configurations
    private Map<String, String> vertexLabels = new HashMap<>();
    private Map<String, String> edgeLabels = new HashMap<>();

    // ID transformation configurations
    private Map<String, String> vertexIdTransformation = new HashMap<>();
    private Map<String, String> edgeIdTransformation = new HashMap<>();

    // Skip configurations
    private SkipVertices skipVertices = new SkipVertices();
    private SkipEdges skipEdges = new SkipEdges();

    /**
     * Nested class for skipVertices configuration
     */
    @Data
    @NoArgsConstructor
    public static class SkipVertices {
        private Set<String> byId = new HashSet<>();
        private Set<String> byLabel = new HashSet<>();
    }

    /**
     * Nested class for skipEdges configuration
     */
    @Data
    @NoArgsConstructor
    public static class SkipEdges {
        private Set<String> byLabel = new HashSet<>();
    }

    /**
     * Factory method to create LabelMappingConfig from YAML file using automatic object mapping
     */
    public static ConversionConfig fromFile(File yamlFile) throws IOException {
        if (yamlFile == null || !yamlFile.exists()) {
            return new ConversionConfig(); // Return empty config if no file provided
        }

        Constructor constructor = new Constructor(ConversionConfig.class, new LoaderOptions());
        Yaml yaml = new Yaml(constructor);

        try (FileInputStream inputStream = new FileInputStream(yamlFile)) {
            ConversionConfig config = yaml.load(inputStream);

            // Handle null case if YAML file is empty or malformed
            if (config == null) {
                config = new ConversionConfig();
            }

            // Ensure nested objects are initialized when yaml fields are left empty
            if (config.skipVertices == null) {
                config.skipVertices = new SkipVertices();
            }
            if (config.skipEdges == null) {
                config.skipEdges = new SkipEdges();
            }
            if (config.vertexLabels == null) {
                config.vertexLabels = new HashMap<>();
            }
            if (config.edgeLabels == null) {
                config.edgeLabels = new HashMap<>();
            }
            if (config.vertexIdTransformation == null) {
                config.vertexIdTransformation = new HashMap<>();
            }
            if (config.edgeIdTransformation == null) {
                config.edgeIdTransformation = new HashMap<>();
            }

            // Ensure nested sets are initialized
            if (config.skipVertices.byId == null) {
                config.skipVertices.byId = new HashSet<>();
            }
            if (config.skipVertices.byLabel == null) {
                config.skipVertices.byLabel = new HashSet<>();
            }
            if (config.skipEdges.byLabel == null) {
                config.skipEdges.byLabel = new HashSet<>();
            }

            return config;
        }
    }

    /**
     * Checks if any skip rules are configured.
     */
    public boolean hasSkipRules() {
        return !skipVertices.byId.isEmpty() || !skipVertices.byLabel.isEmpty() || !skipEdges.byLabel.isEmpty();
    }

    /**
     * Checks if any ID transformations are configured.
     */
    public boolean hasIdTransformations() {
        return !vertexIdTransformation.isEmpty() || !edgeIdTransformation.isEmpty();
    }
}
