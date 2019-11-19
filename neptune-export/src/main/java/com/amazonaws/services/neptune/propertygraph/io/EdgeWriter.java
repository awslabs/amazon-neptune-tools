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

import java.io.IOException;
import java.util.Map;

public class EdgeWriter implements GraphElementHandler<Map<String, Object>> {

    private final PropertyGraphPrinter propertyGraphPrinter;

    public EdgeWriter(PropertyGraphPrinter propertyGraphPrinter) {
        this.propertyGraphPrinter = propertyGraphPrinter;
    }

    @Override
    public void handle(Map<String, Object> map, boolean allowTokens) throws IOException {
        String from = String.valueOf(map.get("from"));
        String to = String.valueOf(map.get("to"));
        Map<?, Object> properties = (Map<?, Object>) map.get("properties");
        String id = (String) map.get("id");
        String label = (String) map.get("label");

        propertyGraphPrinter.printStartRow();
        propertyGraphPrinter.printEdge(id, label, from, to);
        propertyGraphPrinter.printProperties(id, "ep", properties);
        propertyGraphPrinter.printEndRow();
    }

    @Override
    public void close() throws Exception {
        propertyGraphPrinter.close();
    }
}
