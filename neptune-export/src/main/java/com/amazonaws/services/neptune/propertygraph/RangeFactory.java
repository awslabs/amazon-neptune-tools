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

package com.amazonaws.services.neptune.propertygraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;

public class RangeFactory {

    private static final Logger logger = LoggerFactory.getLogger(RangeFactory.class);

    public static RangeFactory create(GraphClient<?> graphClient,
                                      LabelsFilter labelsFilter,
                                      RangeConfig rangeConfig,
                                      ConcurrencyConfig concurrencyConfig) {
        long elementCount = graphClient.count(labelsFilter);

        if (concurrencyConfig.isUnboundedParallelExecution(rangeConfig)) {
            logger.info("Calculating " + graphClient.description() + " ranges");
            long limit = min(elementCount, rangeConfig.limit());
            long rangeSize = (limit / concurrencyConfig.concurrency()) + 1;
            logger.info("Limit: " + limit + ", Size: " + rangeSize);
            return new RangeFactory(rangeSize, limit, rangeConfig.skip(), elementCount);
        } else {
            return new RangeFactory(rangeConfig.rangeSize(), rangeConfig.limit(), rangeConfig.skip(), elementCount);
        }
    }

    private final long rangeSize;
    private final long rangeLimit;
    private final AtomicLong currentEnd;

    private RangeFactory(long rangeSize, long limit, long skip, long max) {
        this.rangeSize = rangeSize;
        this.rangeLimit = min((limit + skip), max) ;
        this.currentEnd =  new AtomicLong(skip);
    }

    public Range nextRange() {
        long proposedEnd = currentEnd.accumulateAndGet(rangeSize, (left, right) -> left+right);

        long start = min(proposedEnd - rangeSize, rangeLimit);
        long actualEnd = min(proposedEnd, rangeLimit);

        return new Range(start, actualEnd);
    }

    public boolean isExhausted() {
        return currentEnd.get() >= rangeLimit;
    }
}
