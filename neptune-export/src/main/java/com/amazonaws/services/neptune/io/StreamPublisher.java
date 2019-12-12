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
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamPublisher implements Runnable {

    private final BlockingQueue<String> queue;
    private final KinesisProducer kinesisProducer;
    private final String streamName;
    private final CountDownLatch latch = new CountDownLatch(1);

    private volatile boolean allowContinue = true;

    private static final Logger logger = LoggerFactory.getLogger(StreamPublisher.class);

    public StreamPublisher(BlockingQueue<String> queue, KinesisProducer kinesisProducer, String streamName) {
        this.queue = queue;
        this.kinesisProducer = kinesisProducer;
        this.streamName = streamName;
    }

    @Override
    public void run() {
        while (allowContinue) {
            processRecord();
        }
        latch.countDown();
    }

    public void stop() {

        allowContinue = false;

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
        }

        while (!queue.isEmpty()) {
            processRecord();
        }

        kinesisProducer.flushSync();
    }

    private void processRecord() {
        try {
            if (kinesisProducer.getOutstandingRecordsCount() > (3000)) {
                Thread.sleep(1);
            }

            String record = queue.poll(1, TimeUnit.SECONDS);
            if (StringUtils.isNotEmpty(record)) {

                ByteBuffer data = ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8.name()));

                ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(streamName, UUID.randomUUID().toString(), data);
                Futures.addCallback(future, callback);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
        @Override
        public void onSuccess(UserRecordResult userRecordResult) {
            if (!userRecordResult.isSuccessful()) {
                logger.error("Unsuccessful attempt to write to stream: " + formatAttempts(userRecordResult.getAttempts()));
            }
        }

        @Override
        public void onFailure(Throwable throwable) {
            if (UserRecordFailedException.class.isAssignableFrom(throwable.getClass())) {
                UserRecordFailedException e = (UserRecordFailedException) throwable;
                logger.error("Error writing to stream: " + formatAttempts(e.getResult().getAttempts()));
            }
            logger.error("Error writing to stream.", throwable);
        }
    };

    private static String formatAttempts(List<Attempt> attempts) {
        StringBuilder builder = new StringBuilder();
        for (Attempt attempt : attempts) {
            builder.append("[");
            builder.append(attempt.getErrorCode()).append(":").append(attempt.getErrorMessage());
            builder.append("(").append(attempt.getDelay()).append(",").append(attempt.getDuration()).append(")");
            builder.append("]");
        }
        return builder.toString();
    }

}
