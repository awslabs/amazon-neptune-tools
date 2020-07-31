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

import com.amazonaws.services.neptune.cluster.ConcurrencyConfig;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeFactoryTest {

    @Test
    public void shouldReturnConsecutiveRanges(){

        GraphClient graphClient = mock(GraphClient.class);
        when(graphClient.approxCount(any(), any())).thenReturn(2250L);

        RangeFactory rangeFactory = RangeFactory.create(
                graphClient,
                AllLabels.INSTANCE,
                new RangeConfig(1000, 0, 2500, -1, -1),
                new ConcurrencyConfig(1));

        Range range1 = rangeFactory.nextRange();
        assertEquals("range(0, 1000)", range1.toString());

        Range range2 = rangeFactory.nextRange();
        assertEquals("range(1000, 2000)", range2.toString());

        Range range3 = rangeFactory.nextRange();
        assertEquals("range(2000, 2500)", range3.toString());
        assertFalse(range3.isEmpty());

        Range range4 = rangeFactory.nextRange();
        assertEquals("range(-1, -1)", range4.toString());
        assertTrue(range4.isEmpty());
    }

    @Test
    public void shouldReturnSingleRangeForAllIfRangeSizeIsMinusOne(){

        GraphClient graphClient = mock(GraphClient.class);
        when(graphClient.approxCount(any(), any())).thenReturn(2250L);

        RangeFactory rangeFactory = RangeFactory.create(
                graphClient,
                AllLabels.INSTANCE,
                new RangeConfig(-1, 0, Long.MAX_VALUE, -1, -1),
                new ConcurrencyConfig(1));

        Range range1 = rangeFactory.nextRange();
        assertEquals("range(0, -1)", range1.toString());
        assertFalse(range1.isEmpty());

        Range range2 = rangeFactory.nextRange();
        assertEquals("range(-1, -1)", range2.toString());
        assertTrue(range2.isEmpty());
    }

    @Test
    public void shouldLeaveLastRangeOpenIfNoUpperLimit(){

        GraphClient graphClient = mock(GraphClient.class);
        when(graphClient.approxCount(any(), any())).thenReturn(2250L);

        RangeFactory rangeFactory = RangeFactory.create(
                graphClient,
                AllLabels.INSTANCE,
                new RangeConfig(1000, 0, Long.MAX_VALUE, -1, -1),
                new ConcurrencyConfig(1));

        Range range1 = rangeFactory.nextRange();
        assertEquals("range(0, 1000)", range1.toString());

        Range range2 = rangeFactory.nextRange();
        assertEquals("range(1000, 2000)", range2.toString());

        Range range3 = rangeFactory.nextRange();
        assertEquals("range(2000, -1)", range3.toString());
        assertFalse(range3.isEmpty());

        Range range4 = rangeFactory.nextRange();
        assertEquals("range(-1, -1)", range4.toString());
        assertTrue(range4.isEmpty());
    }

    @Test
    public void shouldIndicateThatItIsExhausted(){

        GraphClient graphClient = mock(GraphClient.class);
        when(graphClient.approxCount(any(), any())).thenReturn(5000L);

        RangeFactory rangeFactory = RangeFactory.create(
                graphClient,
                AllLabels.INSTANCE,
                new RangeConfig(1000, 0, 2000, -1, -1),
                new ConcurrencyConfig(1));

        rangeFactory.nextRange();
        assertFalse(rangeFactory.isExhausted());

        rangeFactory.nextRange();
        assertTrue(rangeFactory.isExhausted());
    }

    @Test
    public void shouldCalculateRangesStartingFromSkipNumber(){

        GraphClient graphClient = mock(GraphClient.class);
        when(graphClient.approxCount(any(), any())).thenReturn(30L);

        RangeFactory rangeFactory = RangeFactory.create(
                graphClient,
                AllLabels.INSTANCE,
                new RangeConfig(10, 20, 10, -1, -1),
                new ConcurrencyConfig(1));

        Range range1 = rangeFactory.nextRange();
        assertEquals("range(20, 30)", range1.toString());

        assertTrue(rangeFactory.isExhausted());
    }


}