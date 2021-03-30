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
    private final AtomicLong windowSizeBytes = new AtomicLong();
    private volatile long queueHighWatermark = 10000;
    private volatile int tumblingWindowSize = 10;

    private static final long MAX_QUEUE_HIGH_WATERMARK = 10000;
    private static final long QUEUE_SIZE_BYTES = 10000000;
    private static final int LENGTH_HIGH_WATERMARK = 900000;

    public StreamThrottle(KinesisProducer kinesisProducer) {
        this.kinesisProducer = kinesisProducer;
    }

    public void recalculateMaxBufferSize(long counter, long length) {

        long currentWindowSizeBytes = windowSizeBytes.addAndGet(length);

        if (length > LENGTH_HIGH_WATERMARK || counter % tumblingWindowSize == 0) {
            queueHighWatermark = Math.min(QUEUE_SIZE_BYTES / (currentWindowSizeBytes / tumblingWindowSize), MAX_QUEUE_HIGH_WATERMARK);
            logger.debug("Current window has {} records totalling {} bytes, meaning that maxNumberOfQueuedRecords cannot exceed {}", tumblingWindowSize, currentWindowSizeBytes, queueHighWatermark);
            windowSizeBytes.set(0);
        }
    }

    public void throttle() throws InterruptedException {
        if (kinesisProducer.getOutstandingRecordsCount() > (queueHighWatermark)) {
            long start = System.currentTimeMillis();
            while (kinesisProducer.getOutstandingRecordsCount() > (queueHighWatermark)) {
                Thread.sleep(1);
            }
            long end = System.currentTimeMillis();
            logger.debug("Paused adding records to stream for {} millis while number of queued records exceeded maxNumberOfQueuedRecords of {}", end - start, queueHighWatermark);
        }
    }
}
