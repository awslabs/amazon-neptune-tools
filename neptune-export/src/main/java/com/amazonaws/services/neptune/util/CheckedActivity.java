package com.amazonaws.services.neptune.util;

public interface CheckedActivity {
    interface Runnable extends CheckedActivity {
        void run() throws Exception;
    }

    interface Callable<T> extends CheckedActivity{
        T call() throws Exception;
    }
}
