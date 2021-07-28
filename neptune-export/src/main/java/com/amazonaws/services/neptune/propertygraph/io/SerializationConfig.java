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

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SerializationConfig {

    private final String serializer;
    private final int maxContentLength;
    private final int batchSize;
    private final boolean useJanusSerializer;

    public SerializationConfig(String serializer, int maxContentLength, int batchSize, boolean useJanusSerializer) {
        this.serializer = serializer;
        this.maxContentLength = maxContentLength;
        this.batchSize = batchSize;
        this.useJanusSerializer = useJanusSerializer;
    }

    public Cluster.Builder apply(Cluster.Builder builder) {
        Cluster.Builder b = builder.resultIterationBatchSize(batchSize)
                .maxContentLength(maxContentLength);

        if (useJanusSerializer) {
            Map<String, Object> config = new HashMap<>();
            config.put("ioRegistries", Collections.singletonList("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
            MessageSerializer s = new GraphSONMessageSerializerV3d0();
            s.configure(config, null);
            return b.serializer(s);
        } else {
            return b.serializer(serializer);
        }
    }
}
