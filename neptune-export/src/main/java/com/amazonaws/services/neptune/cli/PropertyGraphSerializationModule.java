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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.propertygraph.NeptuneGremlinClient;
import com.amazonaws.services.neptune.propertygraph.io.SerializationConfig;
import com.amazonaws.services.neptune.propertygraph.schema.TokensOnly;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;

public class PropertyGraphSerializationModule {

    @Option(name = {"--serializer"}, description = "Message serializer – (optional, default 'GRAPHBINARY_V1D0').")
    @AllowedEnumValues(Serializers.class)
    @Once
    private String serializer = Serializers.GRAPHBINARY_V1D0.name();

    @Option(name = {"--janus"}, description = "Use JanusGraph serializer.")
    @Once
    private boolean useJanusSerializer = false;

    @Option(name = {"--max-content-length"}, description = "Max content length (optional, default 50000000).")
    @Once
    private int maxContentLength = 50000000;

    @Option(name = {"-b", "--batch-size"}, description = "Batch size (optional, default 64). Reduce this number if your queries trigger CorruptedFrameExceptions.")
    @Once
    private int batchSize = NeptuneGremlinClient.DEFAULT_BATCH_SIZE;

    public SerializationConfig config(){
        return new SerializationConfig(serializer, maxContentLength, batchSize, useJanusSerializer);
    }
}


