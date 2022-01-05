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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.propertygraph.TokenPrefix;
import com.amazonaws.services.neptune.propertygraph.io.CsvPrinterOptions;
import com.amazonaws.services.neptune.propertygraph.io.JsonPrinterOptions;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class PrinterOptionsModule {

    @Option(name = {"--exclude-type-definitions"}, description = "Exclude type definitions from CSV column headers (optional, default 'false').")
    @Once
    private boolean excludeTypeDefinitions = false;

    @Option(name = {"--escape-csv-headers"}, description = "Escape characters in CSV column headers (optional, default 'false').")
    @Once
    private boolean escapeCsvHeaders = false;

    @Option(name = {"--strict-cardinality"}, description = "Format all set and list cardinality properties as arrays in JSON, including properties with a single value (optional, default 'false').")
    @Once
    private boolean strictCardinality = false;

    @Option(name = {"--escape-newline"}, description = "Escape newline characters in CSV files (optional, default 'false').")
    @Once
    private boolean escapeNewline = false;

    @Option(name = {"--multi-value-separator"}, description = "Separator for multi-value properties in CSV output (optional, default ';').")
    @Once
    private String multiValueSeparator = ";";

    @Option(name = {"--token-prefix"}, description = "Token prefix (optional, default '~').")
    @Once
    private String tokenPrefix = "~";

    public PrinterOptions config(){

        CsvPrinterOptions csvPrinterOptions = CsvPrinterOptions.builder()
                .setMultiValueSeparator(multiValueSeparator)
                .setIncludeTypeDefinitions(!excludeTypeDefinitions)
                .setEscapeCsvHeaders(escapeCsvHeaders)
                .setEscapeNewline(escapeNewline)
                .setTokenPrefix(new TokenPrefix(tokenPrefix))
                .build();

        JsonPrinterOptions jsonPrinterOptions = JsonPrinterOptions.builder()
                .setStrictCardinality(strictCardinality)
                .setTokenPrefix(new TokenPrefix(tokenPrefix))
                .build();

        return new PrinterOptions(csvPrinterOptions, jsonPrinterOptions);
    }
}
