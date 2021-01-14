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

public class JsonPrinterOptions {

    public static Builder builder(){
        return new Builder();
    }

    private final boolean strictCardinality;

    private JsonPrinterOptions(boolean strictCardinality) {
        this.strictCardinality = strictCardinality;
    }

    public boolean strictCardinality() {
        return strictCardinality;
    }

    public Builder copy(){
        return new Builder().setStrictCardinality(strictCardinality);
    }

    public static class Builder{
        private boolean strictCardinality = false;

        public Builder setStrictCardinality(boolean strictCardinality) {
            this.strictCardinality = strictCardinality;
            return this;
        }

        public JsonPrinterOptions build(){
            return new JsonPrinterOptions(strictCardinality);
        }

    }


}
