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

import com.amazonaws.services.neptune.cluster.*;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class NeptuneStreamsModule {

    @Option(name = {"--include-last-event-id"}, description = "Get the last event ID from the Amazon Neptune stream, if enabled, and save it to a JSON file (optional, default 'false').")
    @Once
    private boolean includeLastEventId = false;

    public GetLastEventIdStrategy lastEventIdStrategy(Cluster cluster, JsonResource<EventId, Object> eventIdResource){
        if (includeLastEventId){
            return new GetLastEventIdTask(cluster, eventIdResource);
        } else {
            return new DoNotGetLastEventIdTask();
        }
    }


}
