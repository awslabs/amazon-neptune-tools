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

public class StdOutPrintOutputWriter extends PrintOutputWriter {

    private static final String StdOut = "StdOut";

    public StdOutPrintOutputWriter() {
        this(System.out, true);
    }

    private StdOutPrintOutputWriter(Writer out) {
        super(StdOut, out);
    }

    private StdOutPrintOutputWriter(Writer out, boolean autoFlush) {
        super(StdOut, out, autoFlush);
    }

    private StdOutPrintOutputWriter(OutputStream out) {
        super(StdOut, out);
    }

    private StdOutPrintOutputWriter(OutputStream out, boolean autoFlush) {
        super(StdOut, out, autoFlush);
    }

    private StdOutPrintOutputWriter(String fileName) throws FileNotFoundException {
        super(fileName);
    }

    private StdOutPrintOutputWriter(String fileName, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(fileName, csn);
    }

    private StdOutPrintOutputWriter(File file) throws FileNotFoundException {
        super(file);
    }

    private StdOutPrintOutputWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(file, csn);
    }

    @Override
    public void endCommit() {
        flush();
    }

    @Override
    public void close() {
        flush();
    }

}
