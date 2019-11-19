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

package com.amazonaws.services.neptune.propertygraph.io;

public class SerializationConfig {

    private final String serializer;
    private final int maxContentLength;
    private final int batchSize;

    public SerializationConfig(String serializer, int maxContentLength, int batchSize) {
        this.serializer = serializer;
        this.maxContentLength = maxContentLength;
        this.batchSize = batchSize;
    }

    public String serializer() {
        return serializer;
    }

    public int maxContentLength() {
        return maxContentLength;
    }

    public int batchSize() {
        return batchSize;
    }
}
