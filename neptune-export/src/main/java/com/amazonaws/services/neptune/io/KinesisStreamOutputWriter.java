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

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.neptune.propertygraph.NodesClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(KinesisStreamOutputWriter.class);

    private static FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
        @Override
        public void onSuccess(UserRecordResult userRecordResult) {
            if (!userRecordResult.isSuccessful()){
                logger.error("Unsuccessful attempt to write to stream: " + formatAttempts(userRecordResult.getAttempts()));
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            logger.error("Error writing to stream.", throwable);
        }
    };

    private static String formatAttempts(List<Attempt> attempts){
        StringBuilder builder = new StringBuilder();
        for (Attempt attempt : attempts) {
            builder.append("[");
            builder.append(attempt.getErrorCode()).append(":").append(attempt.getErrorMessage());
            builder.append("(").append(attempt.getDelay()).append(",").append(attempt.getDuration()).append(")");
            builder.append("]");
        }
        return builder.toString();
    }

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
                ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(streamName, partitionKey, data);
                Futures.addCallback(future, callback);
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
