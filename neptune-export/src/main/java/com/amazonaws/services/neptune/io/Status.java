package com.amazonaws.services.neptune.io;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Status {
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicBoolean allowContinue = new AtomicBoolean(true);

    public void update(){
        if (counter.incrementAndGet() % 10000 == 0){
            System.err.print(".");
        }
    }

    public boolean allowContinue(){
        return allowContinue.get();
    }

    public void halt(){
        allowContinue.set(false);
    }
}
