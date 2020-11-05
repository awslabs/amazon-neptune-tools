/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.*;
import com.amazonaws.services.neptune.util.CheckedActivity;
import com.amazonaws.services.neptune.util.Timer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RewriteAndMergeCsv implements RewriteCommand {

    private final PropertyGraphTargetConfig targetConfig;

    public RewriteAndMergeCsv(PropertyGraphTargetConfig targetConfig) {
        this.targetConfig = targetConfig;
    }

    @Override
    public MasterLabelSchemas execute(MasterLabelSchemas masterLabelSchemas) throws Exception {
        GraphElementType<?> graphElementType = masterLabelSchemas.graphElementType();

        System.err.println(String.format("Rewriting and merging %s files...", graphElementType.name()));

        return Timer.timedActivity(String.format("rewriting and merging %s files", graphElementType.name()),
                (CheckedActivity.Callable<MasterLabelSchemas>) () ->
                        rewriteFiles(masterLabelSchemas, graphElementType, targetConfig));
    }

    private MasterLabelSchemas rewriteFiles(MasterLabelSchemas masterLabelSchemas,
                                            GraphElementType<?> graphElementType,
                                            PropertyGraphTargetConfig targetConfig) throws Exception {

        Map<Label, MasterLabelSchema> updatedSchemas = new HashMap<>();

        for (MasterLabelSchema masterLabelSchema : masterLabelSchemas.schemas()) {
            updatedSchemas.put(
                        masterLabelSchema.labelSchema().label(),
                        rewriteAndMerge(targetConfig, graphElementType, masterLabelSchema));
        }

        return new MasterLabelSchemas(updatedSchemas, graphElementType);
    }

    private MasterLabelSchema rewriteAndMerge(PropertyGraphTargetConfig targetConfig,
                                              GraphElementType<?> graphElementType,
                                              MasterLabelSchema masterLabelSchema) throws Exception {

        LabelSchema masterSchema = masterLabelSchema.labelSchema();
        masterSchema.initStats();

        String filename = Directories.fileName(String.format("%s.%s",
                masterSchema.label().fullyQualifiedLabel(),
                targetConfig.format().suffix()));

        RenameableFiles renameableFiles = new RenameableFiles();

        try (PropertyGraphPrinter printer = graphElementType.writerFactory().createPrinter(
                filename,
                masterSchema,
                targetConfig.forFileConsolidation())) {

            renameableFiles.add(new File(printer.outputId()), filename);

            for (FileSpecificLabelSchema fileSpecificLabelSchema : masterLabelSchema.fileSpecificLabelSchemas()) {

                try (DeletableFile file = new DeletableFile(new File(fileSpecificLabelSchema.outputId()))) {

                    LabelSchema labelSchema = fileSpecificLabelSchema.labelSchema();
                    Label label = labelSchema.label();

                    String[] additionalElementHeaders = label.hasFromAndToLabels() ?
                            new String[]{"~fromLabels", "~toLabels"} :
                            new String[]{};

                    String[] filePropertyHeaders =
                            labelSchema.propertySchemas().stream()
                                    .map(p -> p.property().toString())
                                    .collect(Collectors.toList())
                                    .toArray(new String[]{});

                    String[] fileHeaders = ArrayUtils.addAll(
                            graphElementType.tokenNames().toArray(new String[]{}),
                            ArrayUtils.addAll(additionalElementHeaders, filePropertyHeaders));

                    try (Reader in = file.reader()) {

                        CSVFormat format = CSVFormat.RFC4180.withHeader(fileHeaders);
                        Iterable<CSVRecord> records = format.parse(in);

                        for (CSVRecord record : records) {
                            printer.printStartRow();

                            if (graphElementType.equals(GraphElementTypes.Nodes)) {
                                printer.printNode(record.get("~id"), Arrays.asList(record.get("~label").split(";")));
                            } else {
                                if (label.hasFromAndToLabels()) {
                                    printer.printEdge(
                                            record.get("~id"),
                                            record.get("~label"),
                                            record.get("~from"),
                                            record.get("~to"),
                                            Arrays.asList(record.get("~fromLabels").split(";")),
                                            Arrays.asList(record.get("~toLabels").split(";")));
                                } else {
                                    printer.printEdge(record.get("~id"), record.get("~label"), record.get("~from"), record.get("~to"));
                                }
                            }

                            printer.printProperties(record.toMap(), false);
                            printer.printEndRow();
                        }

                    }
                }
            }

        }

        renameableFiles.rename();

        return new MasterLabelSchema(
                masterSchema,
                Collections.singletonList(new FileSpecificLabelSchema(filename, targetConfig.format(), masterSchema)));
    }

}
