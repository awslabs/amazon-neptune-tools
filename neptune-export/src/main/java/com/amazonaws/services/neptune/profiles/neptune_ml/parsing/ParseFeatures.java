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

package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.TrainingJobWriterConfig;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class ParseFeatures {

    private final Collection<JsonNode> nodes;

    public ParseFeatures(Collection<JsonNode> nodes) {
        this.nodes = nodes;
    }

    public void validate() {
        for (JsonNode node : nodes) {
            if (!isWord2VecNodeFeature(node) && !isNumericalBucketFeature(node)) {
                throw new IllegalArgumentException("Illegal feature element: expected word2vec or numerical bucket feature definitions");
            }
        }
    }

    public Collection<TrainingJobWriterConfig.Word2VecConfig> parseWord2VecNodeFeatures() {
        Collection<TrainingJobWriterConfig.Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isWord2VecNodeFeature(node)) {
                String description = "word2vec feature";
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String property = new ParseProperty(node, description).parseSingleColumn();
                String language = new ParseLanguage(node).parseLanguage();
                TrainingJobWriterConfig.Word2VecConfig config = new TrainingJobWriterConfig.Word2VecConfig(nodeType, property, Arrays.asList(language));
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> parseNumericalBucketFeatures() {
        Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            if (isNumericalBucketFeature(node)) {
                String description = "numerical bucket feature";
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String property = new ParseProperty(node, description).parseSingleColumn();
                TrainingJobWriterConfig.Range range = new ParseRange(node, description).parseRange();
                int bucketCount = new ParseBucketCount(node, description).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSize(node, description).parseSlideWindowSize();
                TrainingJobWriterConfig.NumericalBucketFeatureConfig config = new TrainingJobWriterConfig.NumericalBucketFeatureConfig(nodeType, property, range, bucketCount, slideWindowSize);
                numericalBucketFeatures.add(config);
            }
        }
        return numericalBucketFeatures;
    }

    private boolean isWord2VecNodeFeature(JsonNode node) {
        return node.has("node") && node.has("type") && node.get("type").textValue().equalsIgnoreCase("word2vec");
    }

    private boolean isNumericalBucketFeature(JsonNode node) {
        return node.has("node") && node.has("type") && node.get("type").textValue().equalsIgnoreCase("bucket_numerical");
    }
}
