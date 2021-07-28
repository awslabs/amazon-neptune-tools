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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ErrorMessageHelper;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TaskTypeV2;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class ParseTaskTypeV2 {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseTaskTypeV2(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public TaskTypeV2 parseTaskType() {
        if (json.has("task_type")) {
            String taskType = json.get("task_type").textValue();
            try {
                return TaskTypeV2.valueOf(taskType);
            } catch (IllegalArgumentException e) {
                throw ErrorMessageHelper.invalidFieldValue("task_type", taskType, context, taskTypeNames());
            }
        }

        throw ErrorMessageHelper.errorParsingField("task_type", context, "one of the following values: " + ErrorMessageHelper.quoteList(taskTypeNames()));
    }

    private Collection<String> taskTypeNames() {
        Collection<String> results = new ArrayList<>();
        for (TaskTypeV2 taskTypeV2 : TaskTypeV2.values()) {
            results.add(taskTypeV2.name());
        }
        return results;
    }

}
