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

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class RangeFactoryTest {

    @Test
    public void singleThreadedWithNoUpperLimitReturnsConsecutiveRanges(){

        RangeFactory rangeFactory = RangeFactory.create(
                mock(GraphClient.class),
                AllLabels.INSTANCE,
                new ConcurrencyConfig(1, 1000, 0, 2500));

        Range range1 = rangeFactory.nextRange();
        assertEquals("range(0, 1000)", range1.toString());

        Range range2 = rangeFactory.nextRange();
        assertEquals("range(1000, 2000)", range2.toString());

        Range range3 = rangeFactory.nextRange();
        assertEquals("range(2000, 2500)", range3.toString());
    }

    @Test
    public void shouldIndicateThatItIsExhausted(){
        RangeFactory rangeFactory = RangeFactory.create(
                mock(GraphClient.class),
                AllLabels.INSTANCE,
                new ConcurrencyConfig(1, 1000, 0, 2000));

        rangeFactory.nextRange();
        assertFalse(rangeFactory.isExhausted());

        rangeFactory.nextRange();
        assertTrue(rangeFactory.isExhausted());
    }

    @Test
    public void shouldCalculateRangesStartingFromSkipNumber(){
        RangeFactory rangeFactory = RangeFactory.create(
                mock(GraphClient.class),
                AllLabels.INSTANCE,
                new ConcurrencyConfig(1, 10, 20, 10));

        Range range1 = rangeFactory.nextRange();
        assertEquals("range(20, 30)", range1.toString());

        assertTrue(rangeFactory.isExhausted());
    }


}