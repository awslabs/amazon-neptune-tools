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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Status {
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicBoolean allowContinue = new AtomicBoolean(true);

    public void update(){
        int counterValue = counter.incrementAndGet();
        if (counterValue % 10000 == 0){
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
