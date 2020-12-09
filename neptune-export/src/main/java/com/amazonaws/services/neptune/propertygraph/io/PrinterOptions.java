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

public class PrinterOptions {

    public static final PrinterOptions NO_HEADERS = new PrinterOptions(false, false, false);

    private final boolean includeTypeDefinitions;
    private final boolean escapeCsvHeaders;
    private final boolean includeHeaders;

    public PrinterOptions(boolean includeTypeDefinitions, boolean escapeCsvHeaders){
        this(includeTypeDefinitions, escapeCsvHeaders, false);
    }

    public PrinterOptions(boolean includeTypeDefinitions, boolean escapeCsvHeaders, boolean includeHeaders) {

        this.includeTypeDefinitions = includeTypeDefinitions;
        this.escapeCsvHeaders = escapeCsvHeaders;
        this.includeHeaders = includeHeaders;
    }

    public boolean includeTypeDefinitions() {
        return includeTypeDefinitions;
    }

    public boolean escapeCsvHeaders(){
        return escapeCsvHeaders;
    }

    public boolean includeHeaders() {
        return includeHeaders;
    }

    public PrinterOptions withIncludeHeaders(boolean includeHeaders){
        return new PrinterOptions(includeTypeDefinitions, escapeCsvHeaders, includeHeaders);
    }
}
