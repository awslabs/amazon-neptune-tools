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

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class KinesisStreamOutputWriter extends Writer implements OutputWriter {

    private final String streamName;
    private final KinesisProducer kinesisProducer;

    private StringWriter writer;
    private int opCount;

    public KinesisStreamOutputWriter(KinesisConfig kinesisConfig) {
        this.streamName = kinesisConfig.streamName();
        this.kinesisProducer = kinesisConfig.client();
    }

    @Override
    public void startCommit() {
        writer = new StringWriter();
        writer.write("[");
        opCount = 0;
    }

    @Override
    public void endCommit(String partitionKey) {

        writer.write("]");
        String s = writer.toString();

        if (StringUtils.isNotEmpty(s)) {

            try {
                ByteBuffer data = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8.name()));
                kinesisProducer.addUserRecord(streamName, partitionKey, data);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
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
        if (opCount > 0){
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
        kinesisProducer.flushSync();
    }
}
