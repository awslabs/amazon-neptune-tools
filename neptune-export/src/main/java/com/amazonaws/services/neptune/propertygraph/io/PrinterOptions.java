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

public class PrinterOptions {

    public static final PrinterOptions NULL_OPTIONS = new PrinterOptions(
            CsvPrinterOptions.builder().build(),
            JsonPrinterOptions.builder().build());

    private final CsvPrinterOptions csvPrinterOptions;
    private final JsonPrinterOptions jsonPrinterOptions;

    public PrinterOptions(CsvPrinterOptions csvPrinterOptions) {
        this(csvPrinterOptions, JsonPrinterOptions.builder().build());
    }

    public PrinterOptions(JsonPrinterOptions jsonPrinterOptions) {
        this(CsvPrinterOptions.builder().build(), jsonPrinterOptions);
    }

    public PrinterOptions(CsvPrinterOptions csvPrinterOptions, JsonPrinterOptions jsonPrinterOptions) {
        this.csvPrinterOptions = csvPrinterOptions;
        this.jsonPrinterOptions = jsonPrinterOptions;
    }

    public CsvPrinterOptions csv() {
        return csvPrinterOptions;
    }

    public JsonPrinterOptions json() {
        return jsonPrinterOptions;
    }
}
