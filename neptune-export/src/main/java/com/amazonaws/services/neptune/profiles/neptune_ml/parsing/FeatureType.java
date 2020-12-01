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

import com.amazonaws.services.neptune.propertygraph.Label;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public enum FeatureType {
    category{
        @Override
        void validateHint(JsonNode node, String description, Label label) {
            // Do nothing
        }
    },
    numerical{
        @Override
        void validateHint(JsonNode node, String description, Label label) {
            if (node.has("separator")){
                throw new IllegalArgumentException(String.format("Invalid 'separator' field for %s for '%s': numerical feature properties cannot contain multiple values.", description, label.fullyQualifiedLabel()));
            }
        }
    },
    word2vec{
        @Override
        void validateHint(JsonNode node, String description, Label label) {
            // Do nothing
        }
    },
    bucket_numerical{
        @Override
        void validateHint(JsonNode node, String description, Label label) {
            if (node.has("separator")){
                throw new IllegalArgumentException(String.format("Invalid 'separator' field for %s for '%s': numerical feature properties cannot contain multiple values.", description, label.fullyQualifiedLabel()));
            }
        }
    };

    public void addTo(JsonGenerator generator) throws IOException {
        generator.writeStringField("sub_feat_type", name());
    }

    abstract void validateHint(JsonNode node, String description, Label label);
}
