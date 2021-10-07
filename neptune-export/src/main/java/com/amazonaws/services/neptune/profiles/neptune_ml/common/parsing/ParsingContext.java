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

package com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.propertygraph.Label;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;

public class ParsingContext {
    private final String description;
    private final Label label;
    private final Collection<String> properties;
    private final NeptuneMLSourceDataModel dataModel;

    private ParsingContext(String description, Label label, Collection<String> properties,  NeptuneMLSourceDataModel dataModel) {
        this.description = description;
        this.label = label;
        this.properties = properties;
        this.dataModel = dataModel;
    }

    public ParsingContext(String description, NeptuneMLSourceDataModel dataModel) {
        this(description, null, Collections.emptyList(), dataModel);
    }

    public ParsingContext(String description) {
        this(description, null, Collections.emptyList(), NeptuneMLSourceDataModel.PropertyGraph);
    }

    public ParsingContext withLabel(Label label) {
        return new ParsingContext(description, label, properties, dataModel);
    }

    public ParsingContext withProperties(Collection<String> properties) {
        return new ParsingContext(description, label, properties, dataModel);
    }

    public ParsingContext withProperty(String property) {
        if (StringUtils.isNotEmpty(property)){
            return new ParsingContext(description, label, Collections.singleton(property), dataModel);
        } else {
            return this;
        }
    }

    @Override
    public String toString() {
        if (label != null && properties.size() == 1) {
            return String.format("%s (%s: %s, %s: %s)", description, dataModel.nodeTypeName(),  label.allLabelsAsArrayString(), dataModel.nodeAttributeNameSingular(), properties.iterator().next());
        } else if (label != null && !properties.isEmpty()) {
            return String.format("%s (%s: %s, %s: [%s])", description, dataModel.nodeTypeName(), label.allLabelsAsArrayString(), dataModel.nodeAttributeNamePlural(), String.join(", ", properties));
        } else if (label != null) {
            return String.format("%s (%s: %s)", description, dataModel.nodeTypeName(), label.allLabelsAsArrayString());
        } else {
            return description;
        }
    }
}
