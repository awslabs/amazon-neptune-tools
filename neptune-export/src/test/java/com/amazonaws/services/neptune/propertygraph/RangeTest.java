/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeTest {
    @Test
    public void shouldIndicateThatRangeCoversAll(){
        assertTrue(new Range(0, -1).isAll());
        assertFalse(new Range(0, 1).isAll());
        assertFalse(new Range(-1, -1).isAll());
    }

    @Test
    public void shouldIndicateIfEmpty(){
        assertTrue(new Range(-1, -1).isEmpty());
        assertFalse(new Range(0, 1).isEmpty());
        assertFalse(new Range(0, -1).isEmpty());
    }

    @Test
    public void shouldIndicateIfSizeBiggerThanSuupliedValue(){
        assertTrue(new Range(0, -1).sizeExceeds(100));
        assertTrue(new Range(0, 200).sizeExceeds(100));

        assertFalse(new Range(0, 100).sizeExceeds(100));
        assertFalse(new Range(0, 100).sizeExceeds(200));
        assertFalse(new Range(-1, -1).sizeExceeds(1));
    }
}