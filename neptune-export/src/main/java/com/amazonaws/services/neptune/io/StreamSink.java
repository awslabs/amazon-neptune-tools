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

import java.util.concurrent.*;

public class StreamSink {

    private static final Logger logger = LoggerFactory.getLogger(StreamSink.class);

    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final StreamPublisher streamPublisher;

    public StreamSink(KinesisProducer kinesisProducer, String streamName){
        this.streamPublisher = new StreamPublisher(queue, kinesisProducer, streamName);
        this.executorService.submit(streamPublisher);
    }

    public void add(String record){
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
        }
    }

    public void stop(){
        streamPublisher.stop();
        executorService.shutdown();
    }

}
