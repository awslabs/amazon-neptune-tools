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


package com.amazonaws.services.neptune.export;

public enum JobSize {
    small {
        @Override
        public int maxConcurrency() {
            return 8;
        }
    },

    medium {
        @Override
        public int maxConcurrency() {
            return 32;
        }
    },
    large {
        @Override
        public int maxConcurrency() {
            return 64;
        }
    },
    xlarge {
        @Override
        public int maxConcurrency() {
            return 96;
        }
    };

    public static JobSize parse(String value) {
        try {
            return JobSize.valueOf(value.toLowerCase());

        } catch (IllegalArgumentException e) {
            return small;
        }
    }

    public abstract int maxConcurrency();
}
