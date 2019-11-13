/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph.metadata;

public enum TokensOnly {
    off,
    nodes {
        @Override
        public boolean nodeTokensOnly() {
            return true;
        }

    },
    edges {
        @Override
        public boolean edgeTokensOnly() {
            return true;
        }
    },
    both {
        @Override
        public boolean nodeTokensOnly() {
            return true;
        }

        @Override
        public boolean edgeTokensOnly() {
            return true;
        }
    };

    public boolean nodeTokensOnly() {
        return false;
    }

    public boolean edgeTokensOnly() {
        return false;
    }
}
