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

package com.amazonaws.services.neptune.plugins.dgl.parsing;

import com.amazonaws.services.neptune.plugins.dgl.TrainingJobWriterConfig;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class ParseFeatures {

    private final Collection<JsonNode> nodes;

    public ParseFeatures(Collection<JsonNode> nodes) {
        this.nodes = nodes;
    }

    public void validate() {
        for (JsonNode node : nodes) {
            if (node.has("feat_type") && node.has("sub_feat_type")) {
                String featType = node.get("feat_type").textValue();
                String subFeatType = node.get("sub_feat_type").textValue();
                String description = String.format("feat_type '%s' and sub_feat_type '%s'", featType, subFeatType);
                if (!isWord2VecNodeFeature(featType, subFeatType) && !isNumericalBucketFeature(featType, subFeatType)) {
                    throw new IllegalArgumentException(String.format("Unrecognized field values: %s", description));
                }
            } else {
                throw new IllegalArgumentException("Illegal feature element: expected 'feat_type' and 'sub_feat_type' fields");
            }
        }
    }

    public Collection<TrainingJobWriterConfig.Word2VecConfig> parseWord2VecNodeFeatures() {
        Collection<TrainingJobWriterConfig.Word2VecConfig> word2VecFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            String featType = node.get("feat_type").textValue();
            String subFeatType = node.get("sub_feat_type").textValue();
            String description = String.format("feat_type '%s' and sub_feat_type '%s'", featType, subFeatType);
            if (isWord2VecNodeFeature(featType, subFeatType)) {
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String col = new ParseCols(node, description).parseSingleColumn();
                Collection<String> languages = new ParseLanguages(node, description).parseLanguages();
                TrainingJobWriterConfig.Word2VecConfig config = new TrainingJobWriterConfig.Word2VecConfig(nodeType, col, languages);
                word2VecFeatures.add(config);
            }
        }
        return word2VecFeatures;
    }

    public Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> parseNumericalBucketFeatures() {
        Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
        for (JsonNode node : nodes) {
            String featType = node.get("feat_type").textValue();
            String subFeatType = node.get("sub_feat_type").textValue();
            String description = String.format("feat_type '%s' and sub_feat_type '%s'", featType, subFeatType);
            if (isNumericalBucketFeature(featType, subFeatType)) {
                Label nodeType = new ParseNodeType(node, description).parseNodeType();
                String col = new ParseCols(node, description).parseSingleColumn();
                TrainingJobWriterConfig.Range range = new ParseRange(node, description).parseRange();
                int bucketCount = new ParseBucketCount(node, description).parseBucketCount();
                int slideWindowSize = new ParseSlideWindowSize(node, description).parseSlideWindowSize();
                TrainingJobWriterConfig.NumericalBucketFeatureConfig config = new TrainingJobWriterConfig.NumericalBucketFeatureConfig(nodeType, col, range, bucketCount, slideWindowSize);
                numericalBucketFeatures.add(config);
            }
        }
        return numericalBucketFeatures;
    }

    private boolean isNumericalBucketFeature(String labelType, String subLabelType) {
        return labelType.equals("node") && subLabelType.equals("bucket_numerical");
    }

    private boolean isWord2VecNodeFeature(String labelType, String subLabelType) {
        return labelType.equals("node") && subLabelType.equals("word2vec");
    }
}
