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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Map;

public class QueriesWriterFactory implements WriterFactory<Map<?, ?>> {

    private final Directories directories;

    public QueriesWriterFactory(Directories directories) {
        this.directories = directories;
    }

    @Override
    public Printer createPrinter(String name, int index, Map<String, PropertyTypeInfo> metadata, Format format) throws IOException {
        Path directory = directories.resultsDirectory().resolve(name);
        java.nio.file.Path filePath = directories.createFilePath(directory, name, index, format);
        PrintWriter printWriter = new PrintWriter(new FileWriter(filePath.toFile()));

        return format.createPrinter(printWriter, metadata);
    }

    @Override
    public GraphElementHandler<Map<?, ?>> createLabelWriter(Printer printer) {
        return new QueryWriter(printer);
    }
}
