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

import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;
import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class EdgesWriterFactory implements WriterFactory<Path> {

    @Override
    public Printer createPrinter(String name, int index, Map<Object, PropertyTypeInfo> metadata, TargetConfig targetConfig) throws IOException {
        Printer printer = targetConfig.createPrinterForEdges(name, index, metadata);
        printer.printHeaderMandatoryColumns("~id", "~label", "~from", "~to");

        return printer;
    }

    @Override
    public GraphElementHandler<Path> createLabelWriter(Printer printer) {
        return new EdgeWriter(printer);
    }
}
