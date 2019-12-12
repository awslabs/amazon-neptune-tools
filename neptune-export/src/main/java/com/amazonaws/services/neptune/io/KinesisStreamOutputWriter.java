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

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class KinesisStreamOutputWriter extends Writer implements OutputWriter {


    private final StreamSink streamSink;
    private StringWriter writer;
    private int opCount;

    public KinesisStreamOutputWriter(StreamSink streamSink) {
        this.streamSink = streamSink;
    }

    @Override
    public void startCommit() {

        writer = new StringWriter();
        writer.write("[");
        opCount = 0;
    }

    @Override
    public void endCommit() {

        writer.write("]");
        String s = writer.toString();

        if (StringUtils.isNotEmpty(s)) {
            streamSink.add(s);
        }
    }

    @Override
    public void print(String s) {
        writer.write(s);
    }

    @Override
    public Writer writer() {
        return this;
    }

    @Override
    public void startOp() {
        if (opCount > 0) {
            writer.write(",");
        }
        opCount++;
    }

    @Override
    public void endOp() {

    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        writer.write(cbuf, off, len);
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
        streamSink.stop();
    }
}
