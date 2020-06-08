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
    public PrintOutputWriter(Writer out) {
        super(out);
    }

    public PrintOutputWriter(Writer out, boolean autoFlush) {
        super(out, autoFlush);
    }

    public PrintOutputWriter(OutputStream out) {
        super(out);
    }

    public PrintOutputWriter(OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
    }

    public PrintOutputWriter(String fileName) throws FileNotFoundException {
        super(fileName);
    }

    public PrintOutputWriter(String fileName, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(fileName, csn);
    }

    public PrintOutputWriter(File file) throws FileNotFoundException {
        super(file);
    }

    public PrintOutputWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(file, csn);
    }

    @Override
    public void startCommit() {
        // Do nothing
    }

    @Override
    public void endCommit() {
//        if (checkError()){
//            throw new RuntimeException("An error occurred while writing to a file");
//        }
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

//    @Override
//    public void close() {
//        boolean isError = checkError();
//        super.close();
//        if (isError){
//          throw new RuntimeException("An error occurred while writing to a file");
//        }
//    }
}
