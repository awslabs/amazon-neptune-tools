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

package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.*;

public class TrainingJobConfigBuilder {

    public static TrainingJobConfigBuilder builder() {
        return new TrainingJobConfigBuilder();
    }

    Map<Label, TrainingJobWriterConfig.LabelConfig> nodeClassLabels = new HashMap<>();
    Map<Label, TrainingJobWriterConfig.LabelConfig> edgeClassLabels = new HashMap<>();
    Collection<TrainingJobWriterConfig.Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
    Collection<TrainingJobWriterConfig.NumericalBucketFeatureConfig> numericalBucketFeatures = new ArrayList<>();
    Collection<Double> splitRates = Arrays.asList(0.7, 0.1, 0.2);

    public TrainingJobConfigBuilder withNodeClassLabel(Label label, String column) {
        nodeClassLabels.put(label, new TrainingJobWriterConfig.LabelConfig(column, splitRates));
        return this;
    }

    public TrainingJobConfigBuilder withEdgeClassLabel(Label label, String column) {
        edgeClassLabels.put(label, new TrainingJobWriterConfig.LabelConfig(column, splitRates));
        return this;
    }

    public TrainingJobConfigBuilder withWord2VecNodeFeature(Label label, String column, String... languages) {
        word2VecNodeFeatures.add(new TrainingJobWriterConfig.Word2VecConfig(label, column, Arrays.asList(languages)));
        return this;
    }

    public TrainingJobConfigBuilder withNumericalBucketFeature(Label label, String column, TrainingJobWriterConfig.Range range, int bucketCount, int slideWindowSize) {
        numericalBucketFeatures.add(new TrainingJobWriterConfig.NumericalBucketFeatureConfig(label, column, range, bucketCount, slideWindowSize));
        return this;
    }

    public TrainingJobWriterConfig build() {
        return new TrainingJobWriterConfig(nodeClassLabels, edgeClassLabels, word2VecNodeFeatures, numericalBucketFeatures);
    }
}
