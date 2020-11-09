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

import java.io.*;

public class PrintOutputWriter extends PrintWriter implements OutputWriter {

    private final String outputId;

    public PrintOutputWriter(String outputId, Writer out) {
        super(out);
        this.outputId = outputId;
    }

    PrintOutputWriter(String outputId, Writer out, boolean autoFlush) {
        super(out, autoFlush);
        this.outputId = outputId;
    }

    PrintOutputWriter(String outputId, OutputStream out) {
        super(out);
        this.outputId = outputId;
    }

    PrintOutputWriter(String outputId, OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
        this.outputId = outputId;
    }

    PrintOutputWriter(String fileName) throws FileNotFoundException {
        super(fileName);
        this.outputId = fileName;
    }

    PrintOutputWriter(String fileName, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(fileName, csn);
        this.outputId = fileName;
    }

    PrintOutputWriter(File file) throws FileNotFoundException {
        super(file);
        this.outputId = file.getAbsolutePath();
    }

    PrintOutputWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(file, csn);
        this.outputId = file.getAbsolutePath();
    }

    @Override
    public String outputId() {
        return outputId;
    }

    @Override
    public void startCommit() {
        // Do nothing
    }

    @Override
    public void endCommit() {
        // Do nothing
    }

    @Override
    public Writer writer() {
        return this;
    }

    @Override
    public void startOp() {
        // Do nothing
    }

    @Override
    public void endOp() {
        // Do nothing
    }
}
