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

public class Timer implements AutoCloseable {

    private final long start = System.currentTimeMillis();

    private final String description;
    private final boolean padWithNewlines;

    public Timer(String description) {
        this(description, true);
    }

    public Timer(String description, boolean padWithNewlines) {
        this.description = description;
        this.padWithNewlines = padWithNewlines;
    }

    @Override
    public void close() {
        if (padWithNewlines) {
            System.err.println();
        }
        System.err.println(String.format("Completed %s in %s seconds", description, (System.currentTimeMillis() - start) / 1000));
        if (padWithNewlines) {
            System.err.println();
        }
    }
}
