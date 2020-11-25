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

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
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

        String description = labelsFilter.description(String.format("%ss", graphClient.description()));

        logger.info("Calculating ranges for {}", description);

        long estimatedNumberOfItemsInGraph = graphClient.approxCount(labelsFilter, rangeConfig);
        int effectiveConcurrency =  estimatedNumberOfItemsInGraph < 1000 ?
                1 :
                concurrencyConfig.concurrency();
        long rangeSize = concurrencyConfig.isUnboundedParallelExecution(rangeConfig) ?
                (estimatedNumberOfItemsInGraph / effectiveConcurrency) + 1:
                rangeConfig.rangeSize();

        logger.info("Estimated number of {} to export: {}, Range size: {}, Effective concurrency: {}",
                description,
                estimatedNumberOfItemsInGraph,
                rangeSize,
                effectiveConcurrency);

        return new RangeFactory(
                rangeSize,
                rangeConfig.numberOfItemsToExport(),
                rangeConfig.numberOfItemsToSkip(),
                estimatedNumberOfItemsInGraph,
                effectiveConcurrency);
    }

    private final long rangeSize;
    private final long numberOfItemsToExport;
    private final int concurrency;
    private final long rangeUpperBound;
    private final AtomicLong currentEnd;

    private RangeFactory(long rangeSize,
                         long numberOfItemsToExport,
                         long numberOfItemsToSkip,
                         long estimatedNumberOfItemsInGraph,
                         int concurrency) {
        this.rangeSize = rangeSize;
        this.numberOfItemsToExport = numberOfItemsToExport;
        this.concurrency = concurrency;
        this.rangeUpperBound = numberOfItemsToExport == Long.MAX_VALUE ?
                estimatedNumberOfItemsInGraph :
                numberOfItemsToExport + numberOfItemsToSkip;
        this.currentEnd = new AtomicLong(numberOfItemsToSkip);
    }

    public Range nextRange() {

        if (isExhausted()){
            return new Range(-1, -1);
        }

        long proposedEnd = currentEnd.accumulateAndGet(rangeSize, (left, right) -> left + right);

        long start = min(proposedEnd - rangeSize, rangeUpperBound);
        long actualEnd =  min(proposedEnd, rangeUpperBound);

        if ((proposedEnd >= rangeUpperBound) && (numberOfItemsToExport == Long.MAX_VALUE)){
            actualEnd = -1;
        }

        return new Range(start, actualEnd);

    }

    public boolean isExhausted() {
        long end = currentEnd.get();
        return end == -1 || end >= rangeUpperBound;
    }

    public int concurrency() {
        return concurrency;
    }
}
