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

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public enum FeatureType {
    category,
    numerical,
    word2vec,
    bucket_numerical;

    public void addTo(JsonGenerator generator) throws IOException {
        generator.writeStringField("sub_feat_type", name());
    }

    public static boolean isValid(String s){
        for (FeatureType value : FeatureType.values()) {
            if (value.name().equals(s)){
                return true;
            }
        }
        return false;
    }
}
