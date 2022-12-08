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

import com.amazonaws.services.neptune.propertygraph.io.Jsonizable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class EventId implements Jsonizable {
    private final long commitNum;
    private final long opNum;

    public EventId(long commitNum, long opNum) {
        this.commitNum = commitNum;
        this.opNum = opNum;
    }

    public long commitNum() {
        return commitNum;
    }

    public long opNum() {
        return opNum;
    }

    @Override
    public String toString() {
        return "{ " +
                "\"commitNum\": " + commitNum +
                ", \"opNum\": " + opNum +
                " }";
    }

    @Override
    public JsonNode toJson() {
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put("commitNum", commitNum);
        json.put("opNum", opNum);
        return json;
    }
}
