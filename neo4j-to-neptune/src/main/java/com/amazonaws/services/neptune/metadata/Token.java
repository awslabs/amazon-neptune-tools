/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

public class Token implements Header {

    static final Token ID = new Token("~id");
    static final Token LABEL = new Token("~label");
    static final Token FROM = new Token("~from");
    static final Token TO = new Token("~to");

    private final String name;

    private Token(String name) {
        this.name = name;
    }

    @Override
    public void updateDataType(DataType newDataType) {
        // Do nothing
    }

    @Override
    public void setIsMultiValued(boolean isMultiValued) {
        // Do nothing
    }

    @Override
    public String value() {
        return name;
    }
}
