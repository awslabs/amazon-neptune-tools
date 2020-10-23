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

import com.amazonaws.services.neptune.cli.RequiresSchema;
import com.amazonaws.services.neptune.io.FileExtension;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum PropertyGraphExportFormat implements FileExtension, RequiresSchema {
    json {
        @Override
        public boolean requiresSchema() {
            return true;
        }

        @Override
        public String suffix() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException {
            JsonGenerator generator = new JsonFactory().createGenerator(writer.writer());
            generator.setPrettyPrinter(new MinimalPrettyPrinter(System.lineSeparator()));
            generator.disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
            return new JsonPropertyGraphPrinter(writer, generator, labelSchema);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException {
            return createPrinter(writer, labelSchema, includeTypeDefinitions);
        }

        @Override
        public String description() {
            return "JSON";
        }

        @Override
        public RewriteCommand createRewriteCommand() {
            return RewriteCommand.NULL_COMMAND;
        }
    },
    csv {
        @Override
        public boolean requiresSchema() {
            return true;
        }

        @Override
        public String suffix() {
            return "csv";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) {
            return new CsvPropertyGraphPrinter(writer, labelSchema, true, includeTypeDefinitions);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException {
            return new VariableRowCsvPropertyGraphPrinter(writer, labelSchema);
        }

        @Override
        public String description() {
            return "CSV";
        }

        @Override
        public RewriteCommand createRewriteCommand() {
            return new RewriteCsv();
        }
    },
    csvNoHeaders {
        @Override
        public boolean requiresSchema() {
            return true;
        }

        @Override
        public String suffix() {
            return "csv";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) {
            return new CsvPropertyGraphPrinter(writer, labelSchema, false, includeTypeDefinitions);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException {
            return new VariableRowCsvPropertyGraphPrinter(writer, labelSchema);
        }

        @Override
        public String description() {
            return "CSV (no headers)";
        }

        @Override
        public RewriteCommand createRewriteCommand() {
            return new RewriteCsv();
        }
    },
    neptuneStreamsJson{
        @Override
        public boolean requiresSchema() {
            return false;
        }

        @Override
        public String suffix() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException {
            JsonGenerator generator = new JsonFactory().createGenerator(writer.writer());
            generator.setPrettyPrinter(new MinimalPrettyPrinter(""));
            generator.disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
            return new NeptuneStreamsJsonPropertyGraphPrinter(writer, generator);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException {
            return createPrinter(writer, labelSchema, includeTypeDefinitions);
        }

        @Override
        public String description() {
            return "JSON (Neptune Streams format)";
        }

        @Override
        public RewriteCommand createRewriteCommand() {
            return RewriteCommand.NULL_COMMAND;
        }
    };

    abstract PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException;

    abstract PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, boolean includeTypeDefinitions) throws IOException;

    public abstract String description();

    public abstract RewriteCommand createRewriteCommand();
}
