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

package com.amazonaws.services.neptune.util;

public class Timer {

    public static void timedActivity(String description, Activity.Runnable activity) {
        timedActivity(description, true, activity);
    }

    public static void timedActivity(String description, boolean padWithNewlines, Activity.Runnable activity) {
        long start = System.currentTimeMillis();

        try {
            activity.run();
            printSuccess(description, padWithNewlines, start);
        } catch (Exception e) {
            printFailure(description, padWithNewlines, start);
            throw e;
        }
    }

    public static void timedActivity(String description, CheckedActivity.Runnable activity) throws Exception {
        timedActivity(description, true, activity);
    }

    public static void timedActivity(String description, boolean padWithNewlines, CheckedActivity.Runnable activity) throws Exception {
        long start = System.currentTimeMillis();

        try {
            activity.run();
            printSuccess(description, padWithNewlines, start);
        } catch (Exception e) {
            printFailure(description, padWithNewlines, start);
            throw e;
        }
    }

    public static <T> T timedActivity(String description, Activity.Callable<T> activity) {
        return timedActivity(description, true, activity);
    }

    public static <T> T timedActivity(String description, boolean padWithNewlines, Activity.Callable<T> activity) {
        long start = System.currentTimeMillis();

        try {
            T result = activity.call();
            printSuccess(description, padWithNewlines, start);
            return result;
        } catch (Exception e) {
            printFailure(description, padWithNewlines, start);
            throw e;
        }
    }

    public static <T> T timedActivity(String description, CheckedActivity.Callable<T> activity) throws Exception {
        return timedActivity(description, true, activity);
    }

    public static <T> T timedActivity(String description, boolean padWithNewlines, CheckedActivity.Callable<T> activity) throws Exception {
        long start = System.currentTimeMillis();

        try {
            T result = activity.call();
            printSuccess(description, padWithNewlines, start);
            return result;
        } catch (Exception e) {
            printFailure(description, padWithNewlines, start);
            throw e;
        }
    }

    private static void printSuccess(String description, boolean padWithNewlines, long start) {
        if (padWithNewlines) {
            System.err.println();
        }
        System.err.println(String.format("Completed %s in %s seconds", description, (System.currentTimeMillis() - start) / 1000));
        if (padWithNewlines) {
            System.err.println();
        }
    }

    private static void printFailure(String description, boolean padWithNewlines, long start) {
        if (padWithNewlines) {
            System.err.println();
        }
        System.err.println(String.format("An error occurred while %s. Elapsed time: %s seconds", description, (System.currentTimeMillis() - start) / 1000));
        if (padWithNewlines) {
            System.err.println();
        }
    }
}
