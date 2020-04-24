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

    public StdOutPrintOutputWriter(){
        this(System.out, true);
    }

    public StdOutPrintOutputWriter(Writer out) {
        super(out);
    }

    public StdOutPrintOutputWriter(Writer out, boolean autoFlush) {
        super(out, autoFlush);
    }

    public StdOutPrintOutputWriter(OutputStream out) {
        super(out);
    }

    public StdOutPrintOutputWriter(OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
    }

    public StdOutPrintOutputWriter(String fileName) throws FileNotFoundException {
        super(fileName);
    }

    public StdOutPrintOutputWriter(String fileName, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(fileName, csn);
    }

    public StdOutPrintOutputWriter(File file) throws FileNotFoundException {
        super(file);
    }

    public StdOutPrintOutputWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(file, csn);
    }

    @Override
    public void endCommit() {
        flush();
    }

    @Override
    public void close(){
        flush();
    }

}
