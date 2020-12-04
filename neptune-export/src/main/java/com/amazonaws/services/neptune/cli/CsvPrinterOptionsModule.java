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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class CsvPrinterOptionsModule {

    @Option(name = {"--exclude-type-definitions"}, description = "Exclude type definitions from CSV column headers (optional, default 'false').")
    @Once
    private boolean excludeTypeDefinitions = false;

    @Option(name = {"--escape-csv-headers"}, description = "Escape characters in CSV column headers (optional, default 'false').")
    @Once
    private boolean escapeCsvHeaders = false;

    public PrinterOptions config(){
        return new PrinterOptions(!excludeTypeDefinitions, escapeCsvHeaders);
    }


}
