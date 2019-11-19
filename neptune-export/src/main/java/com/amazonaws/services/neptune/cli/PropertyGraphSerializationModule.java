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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.io.SerializationConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class PropertyGraphSerializationModule {

    @Option(name = {"--serializer"}, description = "Message serializer – either 'GRAPHBINARY_V1D0' or 'GRYO_V3D0' (optional, default 'GRAPHBINARY_V1D0')")
    @AllowedValues(allowedValues = {"GRAPHBINARY_V1D0", "GRYO_V3D0"})
    @Once
    private String serializer = "GRAPHBINARY_V1D0";

    @Option(name = {"--max-content-length"}, description = "Max content length (optional, default 65536)")
    @Once
    private int maxContentLength = 65536;

    @Option(name = {"-b", "--batch-size"}, description = "Batch size (optional, default 64). Reduce this number if your queries trigger CorruptedFrameExceptions.")
    @Once
    private int batchSize = NeptuneGremlinClient.DEFAULT_BATCH_SIZE;

    public SerializationConfig config(){
        return new SerializationConfig(serializer, maxContentLength, batchSize);
    }
}


