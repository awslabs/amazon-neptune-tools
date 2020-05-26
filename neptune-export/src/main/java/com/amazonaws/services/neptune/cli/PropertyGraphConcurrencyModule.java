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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class PropertyGraphConcurrencyModule {

    private final boolean allowConcurrentOperations;

    @Option(name = {"-cn", "--concurrency"}, description = "Concurrency (optional)")
    @Once
    private int concurrency = 1;

    public PropertyGraphConcurrencyModule(){
        this(true);
    }

    public PropertyGraphConcurrencyModule(boolean allowConcurrentOperations) {
        this.allowConcurrentOperations = allowConcurrentOperations;
    }

    public ConcurrencyConfig config(){
        return new ConcurrencyConfig(allowConcurrentOperations ? concurrency : 1);
    }
}
