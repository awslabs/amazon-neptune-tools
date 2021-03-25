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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class StreamThrottle {

    private static final Logger logger = LoggerFactory.getLogger(StreamThrottle.class);

    private final KinesisProducer kinesisProducer;
    private final AtomicLong counter = new AtomicLong();
    private final AtomicLong windowLength = new AtomicLong();
    private volatile long maxRecordCount = 10000;

    private static final long QUEUE_SIZE_BYTES = 10000000;
    private static final long MAX_WINDOW_SIZE = 1000;

    public StreamThrottle(KinesisProducer kinesisProducer) {
        this.kinesisProducer = kinesisProducer;
    }

    public long counterForNextRecord(long length){

        long currentWindowLength = windowLength.addAndGet(length);
        long counterValue = counter.incrementAndGet();

        if (counterValue % MAX_WINDOW_SIZE == 0){
            maxRecordCount = QUEUE_SIZE_BYTES / (currentWindowLength /MAX_WINDOW_SIZE);
            logger.info("{} bytes per {} records – maxRecordCount: {}", currentWindowLength, MAX_WINDOW_SIZE, maxRecordCount);
            windowLength.set(0);
        }

        return counterValue;
    }

    public void throttle() throws InterruptedException {
        if (kinesisProducer.getOutstandingRecordsCount() > (maxRecordCount)) {
            long start = System.currentTimeMillis();
            while (kinesisProducer.getOutstandingRecordsCount() > (maxRecordCount)) {
                Thread.sleep(1);
            }
            long end = System.currentTimeMillis();
            logger.trace("Paused adding records to stream for {} millis while number of queued records exceeded current maxRecordCount of {}", end - start, maxRecordCount);
        }
    }
}
