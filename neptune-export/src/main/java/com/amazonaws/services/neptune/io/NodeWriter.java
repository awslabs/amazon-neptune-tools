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

package com.amazonaws.services.neptune.io;

import org.apache.tinkerpop.gremlin.structure.T;

import java.io.IOException;
import java.util.Map;

public class NodeWriter implements GraphElementHandler<Map<?, Object>> {

    private final Printer printer;

    public NodeWriter(Printer printer) {
        this.printer = printer;
    }


    @Override
    public void handle(Map<?, Object> properties, boolean allowStructuralElements) throws IOException {
        String id = String.valueOf(properties.get(T.id));
        String label = String.valueOf(properties.get(T.label));

        printer.printStartRow();
        printer.printNode(id, label);
        printer.printProperties(properties);
        printer.printEndRow();
    }

    @Override
    public void close() throws Exception {
        printer.close();
    }
}
