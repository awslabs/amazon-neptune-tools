package com.amazonaws.services.neptune.graph;

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
