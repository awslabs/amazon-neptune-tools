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

import com.amazonaws.services.neptune.propertygraph.TokenPrefix;

public class CsvPrinterOptions {

    public static Builder builder(){
        return new Builder();
    }

    private final String multiValueSeparator;
    private final boolean includeTypeDefinitions;
    private final boolean escapeCsvHeaders;
    private final boolean includeHeaders;
    private final boolean isSemicolonSeparator;
    private final boolean escapeNewline;
    private final TokenPrefix tokenPrefix;

    private CsvPrinterOptions(String multiValueSeparator,
                              boolean includeTypeDefinitions,
                              boolean escapeCsvHeaders,
                              boolean includeHeaders,
                              boolean escapeNewline,
                              TokenPrefix tokenPrefix) {
        this.multiValueSeparator = multiValueSeparator;
        this.includeTypeDefinitions = includeTypeDefinitions;
        this.escapeCsvHeaders = escapeCsvHeaders;
        this.includeHeaders = includeHeaders;
        this.escapeNewline = escapeNewline;
        this.isSemicolonSeparator = multiValueSeparator.equalsIgnoreCase(";");
        this.tokenPrefix = tokenPrefix;
    }

    public String multiValueSeparator() {
        return multiValueSeparator;
    }

    public boolean includeTypeDefinitions() {
        return includeTypeDefinitions;
    }

    public boolean escapeCsvHeaders() {
        return escapeCsvHeaders;
    }

    public boolean includeHeaders() {
        return includeHeaders;
    }

    public boolean escapeNewline() {
        return escapeNewline;
    }

    public boolean isSemicolonSeparator() {
        return isSemicolonSeparator;
    }

    public TokenPrefix tokenPrefix() {
        return tokenPrefix;
    }

    public Builder copy(){
        return new Builder()
                .setMultiValueSeparator(multiValueSeparator)
                .setIncludeTypeDefinitions(includeTypeDefinitions)
                .setEscapeCsvHeaders(escapeCsvHeaders)
                .setIncludeHeaders(includeHeaders)
                .setEscapeNewline(escapeNewline)
                .setTokenPrefix(tokenPrefix);
    }

    public static class Builder {

        private String multiValueSeparator = "";
        private boolean includeTypeDefinitions = false;
        private boolean escapeCsvHeaders = false;
        private boolean includeHeaders = false;
        private boolean escapeNewline = false;
        private TokenPrefix tokenPrefix = new TokenPrefix();

        public Builder setMultiValueSeparator(String multiValueSeparator) {
            this.multiValueSeparator = multiValueSeparator;
            return this;
        }

        public Builder setIncludeTypeDefinitions(boolean includeTypeDefinitions) {
            this.includeTypeDefinitions = includeTypeDefinitions;
            return this;
        }

        public Builder setEscapeCsvHeaders(boolean escapeCsvHeaders) {
            this.escapeCsvHeaders = escapeCsvHeaders;
            return this;
        }

        public Builder setIncludeHeaders(boolean includeHeaders) {
            this.includeHeaders = includeHeaders;
            return this;
        }

        public Builder setEscapeNewline(boolean escapeNewline) {
            this.escapeNewline = escapeNewline;
            return this;
        }

        public Builder setTokenPrefix(TokenPrefix tokenPrefix){
            this.tokenPrefix = tokenPrefix;
            return this;
        }

        public CsvPrinterOptions build(){
            return new CsvPrinterOptions(multiValueSeparator, includeTypeDefinitions, escapeCsvHeaders, includeHeaders, escapeNewline, tokenPrefix);
        }

    }


}
