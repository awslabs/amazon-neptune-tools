/*
Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public class Property implements Header {

    private final String name;
    private boolean isMultiValued = false;
    private DataType dataType = DataType.None;

    Property(String name) {
        this.name = name;
    }

    @Override
    public void updateDataType(DataType newDataType) {
        this.dataType = DataType.getBroadestType(dataType, newDataType);
    }

    @Override
    public void setIsMultiValued(boolean isMultiValued) {
        this.isMultiValued = isMultiValued;
    }

    @Override
    public String value() {
        return isMultiValued ?
                String.format("%s%s[]", name, dataType.typeDescription()) :
                String.format("%s%s", name, dataType.typeDescription());
    }

    public String getName() {
        return name;
    }
}
