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

import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.LabelConfigV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.FeatureOverrideConfigV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.NumericalBucketFeatureConfigV1;
import com.amazonaws.services.neptune.profiles.neptune_ml.v1.config.TrainingDataWriterConfigV1;
import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.*;

public class PropertyGraphTrainingDataConfigBuilderV1 {

    public static PropertyGraphTrainingDataConfigBuilderV1 builder() {
        return new PropertyGraphTrainingDataConfigBuilderV1();
    }

    Map<Label, LabelConfigV1> nodeClassLabels = new HashMap<>();
    Map<Label, LabelConfigV1> edgeClassLabels = new HashMap<>();
    Collection<Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
    Collection<NumericalBucketFeatureConfigV1> numericalBucketFeatures = new ArrayList<>();
    Collection<FeatureOverrideConfigV1> nodeFeatureOverrides = new ArrayList<>();
    Collection<FeatureOverrideConfigV1> edgeFeatureOverrides = new ArrayList<>();
    Collection<Double> splitRates = Arrays.asList(0.7, 0.1, 0.2);

    public PropertyGraphTrainingDataConfigBuilderV1 withNodeClassLabel(Label label, String column) {
        nodeClassLabels.put(label, new LabelConfigV1("node_class_label", column, splitRates));
        return this;
    }

    public PropertyGraphTrainingDataConfigBuilderV1 withEdgeClassLabel(Label label, String column) {
        edgeClassLabels.put(label, new LabelConfigV1("edge_class_label", column, splitRates));
        return this;
    }

    public PropertyGraphTrainingDataConfigBuilderV1 withWord2VecNodeFeature(Label label, String column, String... languages) {
        word2VecNodeFeatures.add(new Word2VecConfig(label, column, Arrays.asList(languages)));
        return this;
    }

    public PropertyGraphTrainingDataConfigBuilderV1 withNumericalBucketFeature(Label label, String column, Range range, int bucketCount, int slideWindowSize) {
        numericalBucketFeatures.add(new NumericalBucketFeatureConfigV1(label, column, range, bucketCount, slideWindowSize));
        return this;
    }

    public PropertyGraphTrainingDataConfigBuilderV1 withNodeFeatureOverride(FeatureOverrideConfigV1 override) {
        nodeFeatureOverrides.add(override);
        return this;
    }

    public PropertyGraphTrainingDataConfigBuilderV1 withEdgeFeatureOverride(FeatureOverrideConfigV1 override) {
        edgeFeatureOverrides.add(override);
        return this;
    }

    public TrainingDataWriterConfigV1 build() {
        return new TrainingDataWriterConfigV1("training-job", nodeClassLabels, edgeClassLabels, word2VecNodeFeatures, numericalBucketFeatures, nodeFeatureOverrides, edgeFeatureOverrides);
    }
}
