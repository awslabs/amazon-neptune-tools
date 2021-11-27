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

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.amazonaws.services.neptune.export.FeatureToggles;
import com.amazonaws.services.neptune.io.FileExtension;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;

public enum PropertyGraphExportFormat implements FileExtension {
    json {
        @Override
        public String extension() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            JsonGenerator generator = createJsonGenerator(writer, System.lineSeparator());
            return new JsonPropertyGraphPrinter(writer, generator, labelSchema, printerOptions);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            JsonGenerator generator = createJsonGenerator(writer, System.lineSeparator());
            return new JsonPropertyGraphPrinter(writer, generator, labelSchema, printerOptions,  true);
        }

        @Override
        public String description() {
            return "JSON";
        }

        @Override
        public RewriteCommand createRewriteCommand(PropertyGraphTargetConfig targetConfig, ConcurrencyConfig concurrencyConfig, boolean inferSchema, FeatureToggles featureToggles) {
            return RewriteCommand.NULL_COMMAND;
        }
    },
    csv {
        @Override
        public String extension() {
            return "csv";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) {

            PrinterOptions newPrinterOptions = new PrinterOptions(
                    printerOptions.csv().copy()
                            .setIncludeHeaders(true)
                            .build());

            return new CsvPropertyGraphPrinter(writer, labelSchema, newPrinterOptions);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            return new VariableRowCsvPropertyGraphPrinter(writer, labelSchema, printerOptions);
        }

        @Override
        public String description() {
            return "CSV";
        }

        @Override
        public RewriteCommand createRewriteCommand(PropertyGraphTargetConfig targetConfig, ConcurrencyConfig concurrencyConfig, boolean inferSchema, FeatureToggles featureToggles) {
            if (targetConfig.mergeFiles()) {
                return new RewriteAndMergeCsv(targetConfig, concurrencyConfig, featureToggles);
            } else {
                if (inferSchema) {
                    return new RewriteCsv(targetConfig, concurrencyConfig, featureToggles);
                } else {
                    return RewriteCommand.NULL_COMMAND;
                }
            }
        }
    },
    csvNoHeaders {
        @Override
        public String extension() {
            return "csv";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) {

            PrinterOptions newPrinterOptions = new PrinterOptions(
                    printerOptions.csv().copy()
                            .setIncludeHeaders(false)
                            .build());

            return new CsvPropertyGraphPrinter(writer, labelSchema, newPrinterOptions);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            return new VariableRowCsvPropertyGraphPrinter(writer, labelSchema, printerOptions);
        }

        @Override
        public String description() {
            return "CSV (no headers)";
        }

        @Override
        public RewriteCommand createRewriteCommand(PropertyGraphTargetConfig targetConfig, ConcurrencyConfig concurrencyConfig, boolean inferSchema, FeatureToggles featureToggles) {
            if (targetConfig.mergeFiles()) {
                return new RewriteAndMergeCsv(targetConfig, concurrencyConfig, featureToggles);
            } else {
                if (inferSchema) {
                    return new RewriteCsv(targetConfig, concurrencyConfig, featureToggles);
                } else {
                    return RewriteCommand.NULL_COMMAND;
                }
            }
        }
    },
    neptuneStreamsJson {
        @Override
        public String extension() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            JsonGenerator generator = createJsonGenerator(writer, "");
            return new NeptuneStreamsJsonPropertyGraphPrinter(writer, generator);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            return createPrinter(writer, labelSchema, printerOptions);
        }

        @Override
        public String description() {
            return "JSON (Neptune Streams format)";
        }

        @Override
        public RewriteCommand createRewriteCommand(PropertyGraphTargetConfig targetConfig, ConcurrencyConfig concurrencyConfig, boolean inferSchema, FeatureToggles featureToggles) {
            return RewriteCommand.NULL_COMMAND;
        }
    },
    neptuneStreamsSimpleJson {
        @Override
        public String extension() {
            return "json";
        }

        @Override
        PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            JsonGenerator generator = createJsonGenerator(writer, "");
            return new NeptuneStreamsSimpleJsonPropertyGraphPrinter(writer, generator);
        }

        @Override
        PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException {
            return createPrinter(writer, labelSchema, printerOptions);
        }

        @Override
        public String description() {
            return "JSON (Neptune Streams simple format)";
        }

        @Override
        public RewriteCommand createRewriteCommand(PropertyGraphTargetConfig targetConfig, ConcurrencyConfig concurrencyConfig, boolean inferSchema, FeatureToggles featureToggles) {
            return RewriteCommand.NULL_COMMAND;
        }
    };

    private static JsonGenerator createJsonGenerator(OutputWriter writer, String s) throws IOException {
        JsonGenerator generator = new JsonFactory().createGenerator(writer.writer());
        generator.setPrettyPrinter(new MinimalPrettyPrinter(s));
        generator.disable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
        return generator;
    }

    abstract PropertyGraphPrinter createPrinter(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException;

    abstract PropertyGraphPrinter createPrinterForInferredSchema(OutputWriter writer, LabelSchema labelSchema, PrinterOptions printerOptions) throws IOException;

    public abstract String description();

    public abstract RewriteCommand createRewriteCommand(PropertyGraphTargetConfig targetConfig, ConcurrencyConfig concurrencyConfig, boolean inferSchema, FeatureToggles featureToggles);

    public String replaceExtension(String filename, String replacement){
        return String.format("%s.%s",  FilenameUtils.removeExtension(filename), replacement);
    }

}
