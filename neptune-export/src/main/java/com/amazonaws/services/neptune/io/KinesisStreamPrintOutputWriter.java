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

package com.amazonaws.services.neptune.io;

import java.io.Writer;

public class KinesisStreamPrintOutputWriter extends PrintOutputWriter {

    private int opCount;

    private static final String LINE_SEPARATOR = "";

    KinesisStreamPrintOutputWriter(String outputId, Writer out) {
        super(outputId, out);
    }

    @Override
    public void startCommit() {
        opCount = 0;
        write("[");
    }

    @Override
    public void endCommit() {
        write("]");
        write(System.lineSeparator());
    }

    @Override
    public Writer writer() {
        return this;
    }

    @Override
    public String lineSeparator(){
        return LINE_SEPARATOR;
    }

    @Override
    public void startOp() {
        if (opCount > 0) {
            write(",");
        }
        opCount++;
    }

    @Override
    public void endOp(){
    }
}
