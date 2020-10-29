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

package com.amazonaws.services.neptune.dgl;

import com.amazonaws.services.neptune.propertygraph.Label;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

public class TrainingJobConfigTest {

    @Test
    public void shouldThrowExceptionIfLowOrHighAreNotNumeric(){

        Collection<Collection<?>> values = Arrays.asList(
                Arrays.asList(1, "one"),
                Arrays.asList("one", 1),
                Arrays.asList(true, 1),
                Arrays.asList(1, true)
        );

        for (Collection<?> value : values) {
            Iterator<?> iterator = value.iterator();
            Object low = iterator.next();
            Object high = iterator.next();

            try {
                new TrainingJobConfig.NumericalBucketFeatureConfig(
                        new Label("my-label"),
                        "column", low, high, 10, 2);
                fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e){
                assertEquals("Low and high values must be numeric", e.getMessage());
            }

        }

    }

    @Test
    public void shouldConvertLowOrHighToBroadestType(){


        TrainingJobConfig.NumericalBucketFeatureConfig config1 = new TrainingJobConfig.NumericalBucketFeatureConfig(
                new Label("my-label"),
                "column", 1, 10L, 10, 2);

        assertEquals(Long.class, config1.high().getClass());
        assertEquals(Long.class, config1.low().getClass());

        TrainingJobConfig.NumericalBucketFeatureConfig config2 = new TrainingJobConfig.NumericalBucketFeatureConfig(
                new Label("my-label"),
                "column", 0.1, 10, 10, 2);

        assertEquals(Double.class, config2.high().getClass());
        assertEquals(Double.class, config2.low().getClass());

    }

}