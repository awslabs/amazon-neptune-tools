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

public class NoOpOutputWriter extends PrintOutputWriter {

    private static final String NoOp = "NoOp";

    public NoOpOutputWriter() {
        this(new NoOpOutputStream(), false);
    }

    private NoOpOutputWriter(Writer out) {
        super(NoOp, out);
    }

    private NoOpOutputWriter(Writer out, boolean autoFlush) {
        super(NoOp, out, autoFlush);
    }

    private NoOpOutputWriter(OutputStream out) {
        super(NoOp, out);
    }

    private NoOpOutputWriter(OutputStream out, boolean autoFlush) {
        super(NoOp, out, autoFlush);
    }

    private NoOpOutputWriter(String fileName) throws FileNotFoundException {
        super(fileName);
    }

    private NoOpOutputWriter(String fileName, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(fileName, csn);
    }

    private NoOpOutputWriter(File file) throws FileNotFoundException {
        super(file);
    }

    private NoOpOutputWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
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

    private static class NoOpOutputStream extends OutputStream{

        @Override
        public void write(int b) throws IOException {
            // Do nothing
        }

        @Override
        public void write(byte[] b) throws IOException {
            // Do nothing
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // Do nothing
        }
    }
}
