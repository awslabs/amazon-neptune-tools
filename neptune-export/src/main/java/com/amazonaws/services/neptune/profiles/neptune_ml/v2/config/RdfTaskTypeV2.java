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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.propertygraph.Label;
import org.apache.commons.lang.StringUtils;

public enum RdfTaskTypeV2 {
    classification,
    regression {
        @Override
        public void validate(String predicate, Label label) {
            // Do nothing
        }
    },
    link_prediction {
        @Override
        public void validate(String predicate, Label label) {
            // Do nothing
        }
    };

    public void validate(String predicate, Label label) {
        ParsingContext context = new ParsingContext(String.format("node %s specification", name()), NeptuneMLSourceDataModel.RDF).withLabel(label);
        if (StringUtils.isEmpty(predicate)) {
            throw new IllegalArgumentException(String.format("Missing or empty 'predicate' field for %s.", context));
        }
    }
}
