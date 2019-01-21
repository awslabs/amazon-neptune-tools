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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

public class RangeFactory {

    public static RangeFactory create(GraphClient<?> graphClient, LabelsFilter labelsFilter, ConcurrencyConfig config) {
        if (config.isUnboundedParallelExecution()) {
            System.err.println("Calculating " + graphClient.description() + " range");
            long count = graphClient.count(labelsFilter);
            long range = (count / config.concurrency()) + 1;
            return new RangeFactory(range, count);
        }

        return new RangeFactory(config.range());
    }

    private final long range;
    private final long maxValue;
    private final LongUnaryOperator addRange;
    private final AtomicLong currentEnd = new AtomicLong(0);

    private RangeFactory(long range) {
        this(range, Long.MAX_VALUE);
    }

    private RangeFactory(long range, long maxValue) {
        this.range = range;
        this.maxValue = maxValue;
        this.addRange = v -> v + range;
    }

    public Range nextRange() {

        long end = currentEnd.updateAndGet(addRange);
        long start = end - range;

        return new Range(start, end);
    }

    public boolean exceedsUpperBound(Range range) {
        if (range.start() < 0 || range.start() >= maxValue) {
            return true;
        }

        return false;
    }
}
