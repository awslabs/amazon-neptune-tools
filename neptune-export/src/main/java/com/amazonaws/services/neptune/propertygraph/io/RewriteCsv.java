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

import com.amazonaws.services.neptune.propertygraph.schema.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RewriteCsv implements RewriteCommand {

    @Override
    public void execute(MasterLabelSchema masterLabelSchema,
                        PropertyGraphTargetConfig targetConfig,
                        GraphElementType<?> graphElementType) throws Exception {
        LabelSchema masterSchema = masterLabelSchema.labelSchema();

        for (FileSpecificLabelSchema fileSpecificLabelSchema : masterLabelSchema.fileSpecificLabelSchemas()) {

            File file = new File(fileSpecificLabelSchema.outputId());

            // need to add node or vertex token headers
            String[] filePropertyHeaders =
                    fileSpecificLabelSchema.labelSchema().propertySchemas().stream()
                            .map(p -> p.property().toString())
                            .collect(Collectors.toList())
                            .toArray(new String[]{});

            String[] fileHeaders = ArrayUtils.addAll(
                    graphElementType.tokenNames().toArray(new String[]{}),
                    filePropertyHeaders);

            PropertyGraphPrinter printer = graphElementType.writerFactory().createPrinter(
                    file.getName(),
                    masterSchema,
                    targetConfig.forFileConsolidation());

            Reader in = new FileReader(file);
            CSVFormat format = CSVFormat.RFC4180.withHeader(fileHeaders);
            Iterable<CSVRecord> records = format.parse(in);

            for (CSVRecord record : records) {
                printer.printStartRow();

                if (graphElementType.equals(GraphElementTypes.Nodes)) {
                    printer.printNode(record.get("~id"), Arrays.asList(record.get("~label").split(";")));
                } else {
                    printer.printEdge(record.get("~id"), record.get("~label"), record.get("~from"), record.get("~to"));
                }

                printer.printProperties(record.toMap(), false);
                printer.printEndRow();
            }

            printer.close();

            file.delete();
            new File(printer.outputId()).renameTo(file);
        }
    }
}
