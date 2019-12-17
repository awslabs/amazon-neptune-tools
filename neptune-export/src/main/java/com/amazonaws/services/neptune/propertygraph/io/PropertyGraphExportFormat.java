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

import com.amazonaws.services.neptune.cli.RequiresMetadata;
import com.amazonaws.services.neptune.io.FileExtension;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.propertygraph.metadata.PropertyTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

import java.io.IOException;
import java.util.Map;

public enum PropertyGraphExportFormat implements FileExtension, RequiresMetadata {
    json {
        @Override
        public boolean requiresMetadata() {
            return true;
        }

        @Override
        public String suffix() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, Map<Object, PropertyTypeInfo> metadata, boolean includeTypeDefinitions) throws IOException {
            JsonGenerator generator = new JsonFactory().createGenerator(writer.writer());
            generator.setPrettyPrinter(new MinimalPrettyPrinter(System.lineSeparator()));
            generator.disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
            return new JsonPropertyGraphPrinter(writer, generator, metadata);
        }

        @Override
        public String description() {
            return "JSON";
        }
    },
    csv {
        @Override
        public boolean requiresMetadata() {
            return true;
        }

        @Override
        public String suffix() {
            return "csv";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, Map<Object, PropertyTypeInfo> metadata, boolean includeTypeDefinitions) {
            return new CsvPropertyGraphPrinter(writer, metadata, true, includeTypeDefinitions);
        }

        @Override
        public String description() {
            return "CSV";
        }
    },
    csvNoHeaders {
        @Override
        public boolean requiresMetadata() {
            return true;
        }

        @Override
        public String suffix() {
            return "csv";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, Map<Object, PropertyTypeInfo> metadata, boolean includeTypeDefinitions) {
            return new CsvPropertyGraphPrinter(writer, metadata, false, includeTypeDefinitions);
        }

        @Override
        public String description() {
            return "CSV (no headers)";
        }
    },
    neptuneStreamsJson{
        @Override
        public boolean requiresMetadata() {
            return false;
        }

        @Override
        public String suffix() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, Map<Object, PropertyTypeInfo> metadata, boolean includeTypeDefinitions) throws IOException {
            JsonGenerator generator = new JsonFactory().createGenerator(writer.writer());
            generator.setPrettyPrinter(new MinimalPrettyPrinter(""));
            generator.disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
            return new NeptuneStreamsJsonPropertyGraphPrinter(writer, generator);
        }

        @Override
        public String description() {
            return "JSON (Neptune Streams format)";
        }
    };

    abstract PropertyGraphPrinter createPrinter(OutputWriter writer, Map<Object, PropertyTypeInfo> metadata, boolean includeTypeDefinitions) throws IOException;

    public abstract String description();
}
