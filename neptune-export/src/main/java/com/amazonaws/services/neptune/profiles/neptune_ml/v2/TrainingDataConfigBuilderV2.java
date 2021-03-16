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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.LabelConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Range;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.*;
import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.*;

public class TrainingDataConfigBuilderV2 {

    public static TrainingDataConfigBuilderV2 builder() {
        return new TrainingDataConfigBuilderV2();
    }

    Map<Label, LabelConfig> nodeClassLabels = new HashMap<>();
    Map<Label, LabelConfig> edgeClassLabels = new HashMap<>();
    Collection<TfIdfConfigV2> tfIdfNodeFeatures = new ArrayList<>();
    Collection<DatetimeConfigV2> datetimeNodeFeatures = new ArrayList<>();
    Collection<Word2VecConfig> word2VecNodeFeatures = new ArrayList<>();
    Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures = new ArrayList<>();
    Collection<FeatureOverrideConfigV2> nodeFeatureOverrides = new ArrayList<>();
    Collection<FeatureOverrideConfigV2> edgeFeatureOverrides = new ArrayList<>();
    Collection<Double> splitRates = Arrays.asList(0.8, 0.2, 0.0);

    public TrainingDataConfigBuilderV2 withNodeClassLabel(Label label, String column) {
        nodeClassLabels.put(label, new LabelConfig("node_class_label", column, splitRates));
        return this;
    }

    public TrainingDataConfigBuilderV2 withEdgeClassLabel(Label label, String column) {
        edgeClassLabels.put(label, new LabelConfig("edge_class_label", column, splitRates));
        return this;
    }

    public TrainingDataConfigBuilderV2 withTfIdfNodeFeature(Label label, String column, Range ngramRange, int minDf, int maxFeatures) {
        tfIdfNodeFeatures.add(new TfIdfConfigV2(label, column, ngramRange, minDf, maxFeatures));
        return this;
    }


    public TrainingDataConfigBuilderV2 withDatetimeNodeFeature(Label label, String column, DatetimePartV2... datetimeParts) {
        datetimeNodeFeatures.add(new DatetimeConfigV2(label, column, Arrays.asList(datetimeParts)));
        return this;
    }

    public TrainingDataConfigBuilderV2 withWord2VecNodeFeature(Label label, String column, String... languages) {
        word2VecNodeFeatures.add(new Word2VecConfig(label, column, Arrays.asList(languages)));
        return this;
    }

    public TrainingDataConfigBuilderV2 withNumericalBucketFeature(Label label, String column, Range range, int bucketCount, int slideWindowSize, ImputerTypeV2 imputer) {
        numericalBucketFeatures.add(new NumericalBucketFeatureConfigV2(label, column, range, bucketCount, slideWindowSize, imputer));
        return this;
    }

    public TrainingDataConfigBuilderV2 withNodeFeatureOverride(FeatureOverrideConfigV2 override) {
        nodeFeatureOverrides.add(override);
        return this;
    }

    public TrainingDataConfigBuilderV2 withEdgeFeatureOverride(FeatureOverrideConfigV2 override) {
        edgeFeatureOverrides.add(override);
        return this;
    }

    public TrainingDataWriterConfigV2 build() {
        return new TrainingDataWriterConfigV2("training-data",
                nodeClassLabels,
                edgeClassLabels,
                tfIdfNodeFeatures,
                datetimeNodeFeatures,
                word2VecNodeFeatures,
                numericalBucketFeatures,
                nodeFeatureOverrides,
                edgeFeatureOverrides);
    }

}
