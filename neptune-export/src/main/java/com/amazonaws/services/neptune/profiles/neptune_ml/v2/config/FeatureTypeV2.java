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

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.fasterxml.jackson.databind.JsonNode;

public enum FeatureTypeV2 {
    bucket_numerical {
        @Override
        public void validateOverride(JsonNode json, ParsingContext context) {
            if (json.has("separator")) {
                throw new IllegalArgumentException(String.format("Invalid 'separator' field for %s. Bucket numerical feature property cannot contain multiple values.", context));
            }
        }
    },
    text_word2vec {
        @Override
        public void validateOverride(JsonNode json, ParsingContext context) {
            if (json.has("imputer")) {
                throw new IllegalArgumentException(String.format("Invalid 'imputer' field for %s.", context));
            }
        }
    },
    category {
        @Override
        public void validateOverride(JsonNode json, ParsingContext context) {
            if (json.has("imputer")) {
                throw new IllegalArgumentException(String.format("Invalid 'imputer' field for %s.", context));
            }
        }
    },
    numerical,
    text_tfidf {
        @Override
        public void validateOverride(JsonNode json, ParsingContext context) {
            if (json.has("imputer")) {
                throw new IllegalArgumentException(String.format("Invalid 'imputer' field for %s.", context));
            }
        }
    },
    datetime,
    auto;

    public void validateOverride(JsonNode node, ParsingContext context) {
        //Do nothing
    }

}
