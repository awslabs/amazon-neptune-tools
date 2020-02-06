/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.databind.node.ArrayNode;

public enum MultiValuedNodePropertyPolicy implements PropertyValueParserPolicy {
    LeaveAsString {
        @Override
        public PropertyValue handleArray(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            return parser.stringValue(s);
        }

        @Override
        public void handleDuplicates(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            // Do nothing
        }
    },
    Halt {
        @Override
        public PropertyValue handleArray(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            throw new RuntimeException("Halt: found multivalued node property value");
        }

        @Override
        public void handleDuplicates(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            // Do nothing
        }
    },
    PutInSetIgnoringDuplicates {
        @Override
        public PropertyValue handleArray(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            return parser.parseArrayValue(s, arrayNode);
        }

        @Override
        public void handleDuplicates(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            // Do nothing
        }
    },
    PutInSetButHaltIfDuplicates {
        @Override
        public PropertyValue handleArray(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            return parser.parseArrayValue(s, arrayNode);
        }

        @Override
        public void handleDuplicates(String s, ArrayNode arrayNode, PropertyValueParser parser) {
            throw new RuntimeException("Halt: found multivalued node property value with duplicate values");
        }
    };

    @Override
    public abstract PropertyValue handleArray(String s, ArrayNode arrayNode, PropertyValueParser parser);

    @Override
    public abstract void handleDuplicates(String s, ArrayNode arrayNode, PropertyValueParser parser);
}
