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

public class RangeConfig {

    private final long rangeSize;
    private final long numberOfItemsToSkip;
    private final long numberOfItemsToExport;
    private final long approxNodeCount;
    private final long approxEdgeCount;

    public RangeConfig(long rangeSize,
                       long numberOfItemsToSkip,
                       long numberOfItemsToExport,
                       long approxNodeCount,
                       long approxEdgeCount) {
        this.rangeSize = rangeSize;
        this.numberOfItemsToSkip = numberOfItemsToSkip;
        this.numberOfItemsToExport = numberOfItemsToExport;
        this.approxNodeCount = approxNodeCount;
        this.approxEdgeCount = approxEdgeCount;
    }

    public long rangeSize() {
        return rangeSize;
    }

    public long numberOfItemsToSkip() {
        return numberOfItemsToSkip;
    }

    public long numberOfItemsToExport() {
        return numberOfItemsToExport;
    }

    public long approxNodeCount() {
        return approxNodeCount;
    }

    public long approxEdgeCount() {
        return approxEdgeCount;
    }
}
