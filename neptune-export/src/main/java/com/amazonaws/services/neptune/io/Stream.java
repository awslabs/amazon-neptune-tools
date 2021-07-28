/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Stream {

    private final KinesisProducer kinesisProducer;
    private final String streamName;
    private final StreamThrottle streamThrottle;
    private final LargeStreamRecordHandlingStrategy largeStreamRecordHandlingStrategy;
    private final RecordSplitter splitter;
    private final AtomicLong counter = new AtomicLong();

    private static final Logger logger = LoggerFactory.getLogger(Stream.class);
    private static final int MAX_SIZE_BYTES = 1000000;

    public Stream(KinesisProducer kinesisProducer,
                  String streamName,
                  LargeStreamRecordHandlingStrategy largeStreamRecordHandlingStrategy) {
        this.kinesisProducer = kinesisProducer;
        this.streamName = streamName;
        this.streamThrottle = new StreamThrottle(kinesisProducer);
        this.largeStreamRecordHandlingStrategy = largeStreamRecordHandlingStrategy;
        this.splitter = new RecordSplitter(MAX_SIZE_BYTES, largeStreamRecordHandlingStrategy);
    }

    public synchronized void publish(String s) {

        if (StringUtils.isNotEmpty(s) && s.length() > 2) {

            try {
                long partitionKeyValue = counter.incrementAndGet();
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8.name());

                if (bytes.length > MAX_SIZE_BYTES && largeStreamRecordHandlingStrategy.allowSplit()) {
                    Collection<String> splitRecords = splitter.split(s);
                    for (String splitRecord : splitRecords) {
                        publish(partitionKeyValue, splitRecord.getBytes(StandardCharsets.UTF_8.name()));
                    }
                } else {
                    publish(partitionKeyValue, bytes);
                }


            } catch (UnsupportedEncodingException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void publish(long partitionKeyValue, byte[] bytes) {

        if (bytes.length > MAX_SIZE_BYTES) {
            logger.warn("Dropping record because it is larger than 1 MB: [{}] '{}...'", bytes.length, new String(Arrays.copyOfRange(bytes, 0, 256)));
            return;
        }

        try {
            ByteBuffer data = ByteBuffer.wrap(bytes);

            streamThrottle.recalculateMaxBufferSize(partitionKeyValue, bytes.length);
            streamThrottle.throttle();

            ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(streamName, String.valueOf(partitionKeyValue), data);
            Futures.addCallback(future, CALLBACK, MoreExecutors.directExecutor());

        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public String name() {
        return streamName;
    }

    public void flushRecords() {
        kinesisProducer.flushSync();
    }

    private static final FutureCallback<UserRecordResult> CALLBACK = new FutureCallback<UserRecordResult>() {
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
