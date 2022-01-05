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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Status {

    private static final Logger logger = LoggerFactory.getLogger(Status.class);

    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicBoolean allowContinue = new AtomicBoolean(true);
    private final StatusOutputFormat outputFormat;
    private final String description;
    private final Supplier<String> additionalDetailsSupplier;

    public Status(StatusOutputFormat outputFormat) {
        this(outputFormat, "");
    }

    public Status(StatusOutputFormat outputFormat, String description) {
        this(outputFormat, description, () -> "");
    }

    public Status(StatusOutputFormat outputFormat, String description, Supplier<String> additionalDetailsSupplier) {
        this.outputFormat = outputFormat;
        this.description = description;
        this.additionalDetailsSupplier = additionalDetailsSupplier;
    }

    public void update() {
        int counterValue = counter.incrementAndGet();
        if (counterValue % 10000 == 0 && outputFormat == StatusOutputFormat.Dot) {
            System.err.print(".");
        } else if (counterValue % 100000 == 0 && outputFormat == StatusOutputFormat.Description) {
            logger.info("{} ({}){}", counterValue, description, additionalDetailsSupplier.get());
        }
    }

    public boolean allowContinue() {
        return allowContinue.get();
    }

    public void halt() {
        allowContinue.set(false);
    }
}
