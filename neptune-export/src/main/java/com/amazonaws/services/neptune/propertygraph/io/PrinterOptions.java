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

    public static final PrinterOptions NULL_OPTIONS = new PrinterOptions(false, false, false, false);

    private final boolean includeTypeDefinitions;
    private final boolean escapeCsvHeaders;
    private final boolean includeHeaders;
    private final boolean strictCardinality;

    public PrinterOptions(boolean includeTypeDefinitions, boolean escapeCsvHeaders, boolean strictCardinality){
        this(includeTypeDefinitions, escapeCsvHeaders, strictCardinality, false);
    }

    public PrinterOptions(boolean includeTypeDefinitions, boolean escapeCsvHeaders, boolean strictCardinality, boolean includeHeaders) {

        this.includeTypeDefinitions = includeTypeDefinitions;
        this.escapeCsvHeaders = escapeCsvHeaders;
        this.includeHeaders = includeHeaders;
        this.strictCardinality = strictCardinality;
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

    public boolean strictCardinality() {
        return strictCardinality;
    }

    public PrinterOptions withIncludeHeaders(boolean includeHeaders){
        return new PrinterOptions(includeTypeDefinitions, escapeCsvHeaders, strictCardinality, includeHeaders);
    }


}
