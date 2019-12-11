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
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.apache.commons.lang.StringUtils;

public class KinesisConfig {

    private final String streamName;
    private final KinesisProducer kinesisProducer;

    public KinesisConfig(String streamName, String region) {
        this.streamName = streamName;
        this.kinesisProducer = (StringUtils.isNotEmpty(region) && StringUtils.isNotEmpty(streamName)) ?
                new KinesisProducer(new KinesisProducerConfiguration()
                        .setRegion(region)
                        .setRateLimit(100)
                        .setRecordTtl(Integer.MAX_VALUE)) :
                null;
    }

    public StreamSink createSink() {

        if (kinesisProducer == null){
            throw new IllegalArgumentException("You must supply an AWS Region and Amazon Kinesis Data Stream name");
        }

        return new StreamSink(kinesisProducer, streamName);
    }
}
