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

import com.amazonaws.services.neptune.metadata.PropertyTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class NodesWriterFactory implements WriterFactory<Map<?, Object>> {

    private final Directories directories;

    public NodesWriterFactory(Directories directories) {
        this.directories = directories;
    }

    @Override
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata, Format format) throws IOException {
        java.nio.file.Path filePath = directories.createFilePath(directories.nodesDirectory(), name, index, format);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));

        Printer printer = format.createPrinter(printWriter, metadata);
        printer.printHeaderMandatoryColumns("~id","~label");
        return printer;
    }

    @Override
    public GraphElementHandler<Map<?, Object>> createLabelWriter(Printer printer) {
        return new NodeWriter(printer);
    }
}
