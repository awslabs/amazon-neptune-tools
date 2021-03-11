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

package com.amazonaws.services.neptune.profiles.neptune_ml.v1;

import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.*;

public class TrainingJobConfigBuilderV1 {

    public static TrainingJobConfigBuilderV1 builder() {
        return new TrainingJobConfigBuilderV1();
    }

    Map<Label, TrainingJobWriterConfigV1.LabelConfig> nodeClassLabels = new HashMap<>();
    Map<Label, TrainingJobWriterConfigV1.LabelConfig> edgeClassLabels = new HashMap<>();
    Collection<TrainingJobWriterConfigV1.Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
    Collection<TrainingJobWriterConfigV1.NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
    Collection<TrainingJobWriterConfigV1.FeatureOverrideConfig> nodeFeatureOverrides = new ArrayList<>();
    Collection<TrainingJobWriterConfigV1.FeatureOverrideConfig> edgeFeatureOverrides = new ArrayList<>();
    Collection<Double> splitRates = Arrays.asList(0.7, 0.1, 0.2);

    public TrainingJobConfigBuilderV1 withNodeClassLabel(Label label, String column) {
        nodeClassLabels.put(label, new TrainingJobWriterConfigV1.LabelConfig("node_class_label", column, splitRates));
        return this;
    }

    public TrainingJobConfigBuilderV1 withEdgeClassLabel(Label label, String column) {
        edgeClassLabels.put(label, new TrainingJobWriterConfigV1.LabelConfig("edge_class_label", column, splitRates));
        return this;
    }

    public TrainingJobConfigBuilderV1 withWord2VecNodeFeature(Label label, String column, String... languages) {
        word2VecNodeFeatures.add(new TrainingJobWriterConfigV1.Word2VecConfig(label, column, Arrays.asList(languages)));
        return this;
    }

    public TrainingJobConfigBuilderV1 withNumericalBucketFeature(Label label, String column, TrainingJobWriterConfigV1.Range range, int bucketCount, int slideWindowSize) {
        numericalBucketFeatures.add(new TrainingJobWriterConfigV1.NumericalBucketFeatureConfig(label, column, range, bucketCount, slideWindowSize));
        return this;
    }

    public TrainingJobConfigBuilderV1 withNodeFeatureOverride(TrainingJobWriterConfigV1.FeatureOverrideConfig override) {
        nodeFeatureOverrides.add(override);
        return this;
    }

    public TrainingJobConfigBuilderV1 withEdgeFeatureOverride(TrainingJobWriterConfigV1.FeatureOverrideConfig override) {
        edgeFeatureOverrides.add(override);
        return this;
    }

    public TrainingJobWriterConfigV1 build() {
        return new TrainingJobWriterConfigV1("training-job", nodeClassLabels, edgeClassLabels, word2VecNodeFeatures, numericalBucketFeatures, nodeFeatureOverrides, edgeFeatureOverrides);
    }
}
