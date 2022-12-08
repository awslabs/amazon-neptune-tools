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

package com.amazonaws.services.neptune.cluster;

import com.amazonaws.services.neptune.io.CommandWriter;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class GetLastEventIdTask implements GetLastEventIdStrategy{

    private final Cluster cluster;
    private final JsonResource<EventId> lastEventIdResource;
    private final AtomicReference<EventId> lastEventId = new AtomicReference<>();

    public GetLastEventIdTask(Cluster cluster, JsonResource<EventId> lastEventIdResource) {
        this.cluster = cluster;
        this.lastEventIdResource = lastEventIdResource;
    }

    @Override
    public void saveLastEventId(String streamEndpointType) throws IOException {

        EventId eventId = new GetLastEventId(
                cluster.clusterMetadata(),
                cluster.connectionConfig(),
                streamEndpointType).execute();

        if (eventId != null){
            lastEventId.set(eventId);
            lastEventIdResource.save(eventId);
        }
    }

    @Override
    public void writeLastEventIdResourcePathAsMessage(CommandWriter writer) {
        if (lastEventId.get() != null){
            lastEventIdResource.writeResourcePathAsMessage(writer);
        }
    }
}
