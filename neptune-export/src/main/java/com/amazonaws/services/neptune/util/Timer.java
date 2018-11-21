package com.amazonaws.services.neptune.util;

public class Timer implements AutoCloseable {

    private final long start = System.currentTimeMillis();

    @Override
    public void close() throws Exception {
        System.err.println();
        System.err.println(String.format("Completed in %s seconds", (System.currentTimeMillis() - start) / 1000));
    }
}
