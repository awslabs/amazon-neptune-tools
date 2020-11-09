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

package com.amazonaws.services.neptune.propertygraph.schema;

import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;

public class FileSpecificLabelSchema {

    private final String outputId;
    private final PropertyGraphExportFormat format;
    private final LabelSchema labelSchema;

    public FileSpecificLabelSchema(String outputId,
                                   PropertyGraphExportFormat format,
                                   LabelSchema labelSchema) {
        this.outputId = outputId;
        this.format = format;
        this.labelSchema = labelSchema;
    }

    public String outputId() {
        return outputId;
    }

    public PropertyGraphExportFormat getFormat() {
        return format;
    }

    public LabelSchema labelSchema() {
        return labelSchema;
    }
}
