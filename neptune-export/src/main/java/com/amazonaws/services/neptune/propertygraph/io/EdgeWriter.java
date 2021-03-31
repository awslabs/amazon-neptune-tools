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

import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.LabelsFilter;

import java.io.IOException;
import java.util.*;

public class EdgeWriter implements LabelWriter<Map<String, Object>> {

    private final PropertyGraphPrinter propertyGraphPrinter;
    private final boolean hasFromAndToLabels;

    public EdgeWriter(PropertyGraphPrinter propertyGraphPrinter, Label label) {
        this.propertyGraphPrinter = propertyGraphPrinter;
        this.hasFromAndToLabels = label.hasFromAndToLabels();
    }

    @Override
    public void handle(Map<String, Object> map, boolean allowTokens) throws IOException {
        String from = String.valueOf(map.get("~from"));
        String to = String.valueOf(map.get("~to"));
        @SuppressWarnings("unchecked")
        Map<?, Object> properties = (Map<?, Object>) map.get("properties");
        String id = String.valueOf(map.get("~id"));
        String label = (String) map.get("~label");

        propertyGraphPrinter.printStartRow();

        if (hasFromAndToLabels){
            @SuppressWarnings("unchecked")
            List<String> fromLabels = (List<String>) map.get("~fromLabels");
            @SuppressWarnings("unchecked")
            List<String> toLabels = (List<String>) map.get("~toLabels");

            // Temp fix for concatenated label issue
            fromLabels = Label.fixLabelsIssue(fromLabels);
            toLabels = Label.fixLabelsIssue(toLabels);

            propertyGraphPrinter.printEdge(id, label, from, to, fromLabels, toLabels);
        } else {
            propertyGraphPrinter.printEdge(id, label, from, to);
        }

        propertyGraphPrinter.printProperties(id, "ep", properties);
        propertyGraphPrinter.printEndRow();
    }

    @Override
    public void close() throws Exception {
        propertyGraphPrinter.close();
    }

    @Override
    public String outputId() {
        return propertyGraphPrinter.outputId();
    }
}
