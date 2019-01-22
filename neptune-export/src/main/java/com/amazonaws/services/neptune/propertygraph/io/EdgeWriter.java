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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.IOException;
import java.util.Map;

public class EdgeWriter implements GraphElementHandler<Path> {

    private final Printer printer;

    public EdgeWriter(Printer printer) {
        this.printer = printer;
    }

    @Override
    public void handle(Path path, boolean allowStructuralElements) throws IOException {
        String from = path.get(3);
        String to = path.get(1);
        Map<?, Object> properties = path.get(0);
        String id = String.valueOf(properties.get(T.id));
        String label = String.valueOf(properties.get(T.label));

        printer.printStartRow();
        printer.printEdge(id, label, from, to);
        printer.printProperties(properties);
        printer.printEndRow();
    }

    @Override
    public void close() throws Exception {
        printer.close();
    }
}
