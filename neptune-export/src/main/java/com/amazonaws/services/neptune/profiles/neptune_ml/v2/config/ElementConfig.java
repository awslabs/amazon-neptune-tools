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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.config.Word2VecConfig;
import com.amazonaws.services.neptune.propertygraph.Label;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class ElementConfig {

    public static final ElementConfig EMPTY_CONFIG = new ElementConfig(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    private final Collection<LabelConfigV2> classLabels;
    private final Collection<NoneFeatureConfig> noneFeatures;
    private final Collection<TfIdfConfigV2> tfIdfFeatures;
    private final Collection<DatetimeConfigV2> datetimeFeatures;
    private final Collection<Word2VecConfig> word2VecFeatures;
    private final Collection<FastTextConfig> fastTextFeatures;
    private final Collection<SbertConfig> sbertFeatures;
    private final Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures;
    private final Collection<FeatureOverrideConfigV2> featureOverrides;

    public ElementConfig(Collection<LabelConfigV2> classLabels,
                         Collection<NoneFeatureConfig> noneFeatures,
                         Collection<TfIdfConfigV2> tfIdfFeatures,
                         Collection<DatetimeConfigV2> datetimeFeatures,
                         Collection<Word2VecConfig> word2VecFeatures,
                         Collection<FastTextConfig> fastTextFeatures,
                         Collection<SbertConfig> sbertFeatures,
                         Collection<NumericalBucketFeatureConfigV2> numericalBucketFeatures,
                         Collection<FeatureOverrideConfigV2> featureOverrides) {
        this.classLabels = classLabels;
        this.noneFeatures = noneFeatures;
        this.tfIdfFeatures = tfIdfFeatures;
        this.datetimeFeatures = datetimeFeatures;
        this.word2VecFeatures = word2VecFeatures;
        this.fastTextFeatures = fastTextFeatures;
        this.sbertFeatures = sbertFeatures;
        this.numericalBucketFeatures = numericalBucketFeatures;
        this.featureOverrides = featureOverrides;
    }

    public boolean allowAutoInferFeature(Label label, String property){
        if (hasClassificationSpecificationForProperty(label, property)) {
            return false;
        }
        if (hasNoneFeatureSpecification(label, property)){
            return false;
        }
        if (hasTfIdfSpecification(label, property)){
            return false;
        }
        if (hasDatetimeSpecification(label, property)){
            return false;
        }
        if (hasWord2VecSpecification(label, property)){
            return false;
        }
        if (hasFastTextSpecification(label, property)){
            return false;
        }
        if (hasSbertSpecification(label, property)){
            return false;
        }
        if (hasNumericalBucketSpecification(label, property)){
            return false;
        }
        if (hasFeatureOverrideForProperty(label, property)){
            return false;
        }
        return true;
    }

    public boolean hasClassificationSpecificationsFor(Label label) {
        return !getClassificationSpecifications(label).isEmpty();
    }

    public Collection<LabelConfigV2> getClassificationSpecifications(Label label) {
        return classLabels.stream().filter(c -> c.label().equals(label)).collect(Collectors.toList());
    }

    public boolean hasClassificationSpecificationForProperty(Label label, String property) {
        return getClassificationSpecifications(label).stream().anyMatch(s -> s.property().equals(property));
    }

    public Collection<LabelConfigV2> getAllClassificationSpecifications(){
        return classLabels;
    }

    public boolean hasNoneFeatureSpecification(Label label, String property) {
        return getNoneFeatureSpecification(label, property) != null;
    }

    public NoneFeatureConfig getNoneFeatureSpecification(Label label, String property) {
        return noneFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasTfIdfSpecification(Label label, String property) {
        return getTfIdfSpecification(label, property) != null;
    }

    public TfIdfConfigV2 getTfIdfSpecification(Label label, String property) {
        return tfIdfFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasDatetimeSpecification(Label label, String property) {
        return getDatetimeSpecification(label, property) != null;
    }

    public DatetimeConfigV2 getDatetimeSpecification(Label label, String property) {
        return datetimeFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasWord2VecSpecification(Label label, String property) {
        return getWord2VecSpecification(label, property) != null;
    }

    public Word2VecConfig getWord2VecSpecification(Label label, String property) {
        return word2VecFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasFastTextSpecification(Label label, String property) {
        return getFastTextSpecification(label, property) != null;
    }

    public FastTextConfig getFastTextSpecification(Label label, String property) {
        return fastTextFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasSbertSpecification(Label label, String property) {
        return getSbertSpecification(label, property) != null;
    }

    public SbertConfig getSbertSpecification(Label label, String property) {
        return sbertFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasNumericalBucketSpecification(Label label, String property) {
        return getNumericalBucketSpecification(label, property) != null;
    }

    public NumericalBucketFeatureConfigV2 getNumericalBucketSpecification(Label label, String property) {
        return numericalBucketFeatures.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.property().equals(property))
                .findFirst()
                .orElse(null);
    }

    public boolean hasFeatureOverrideForProperty(Label label, String property) {
        return featureOverrides.stream()
                .anyMatch(override ->
                        override.label().equals(label) &&
                                override.properties().contains(property));
    }

    public Collection<FeatureOverrideConfigV2> getFeatureOverrides(Label label) {
        return featureOverrides.stream()
                .filter(c -> c.label().equals(label))
                .collect(Collectors.toList());
    }

    public FeatureOverrideConfigV2 getFeatureOverride(Label label, String property) {
        return featureOverrides.stream()
                .filter(config ->
                        config.label().equals(label) &&
                                config.properties().contains(property))
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return "ElementConfig{" +
                "classLabels=" + classLabels +
                ", tfIdfFeatures=" + tfIdfFeatures +
                ", datetimeFeatures=" + datetimeFeatures +
                ", word2VecFeatures=" + word2VecFeatures +
                ", numericalBucketFeatures=" + numericalBucketFeatures +
                ", featureOverrides=" + featureOverrides +
                '}';
    }
}
