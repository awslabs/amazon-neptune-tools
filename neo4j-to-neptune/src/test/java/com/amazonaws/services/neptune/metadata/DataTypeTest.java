/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DataTypeTest {

    @Test
    public void shouldIdentifyValidBoolean() {

        assertEquals(DataType.Boolean, DataType.identifyType("true"));
        assertEquals(DataType.Boolean, DataType.identifyType("false"));

        assertNotEquals(DataType.Boolean, DataType.identifyType("yes"));
        assertNotEquals(DataType.Boolean, DataType.identifyType("1"));
    }

    @Test
    public void shouldIdentifyValidByte() {

        assertEquals(DataType.Byte, DataType.identifyType("-128"));
        assertEquals(DataType.Byte, DataType.identifyType("127"));

        assertNotEquals(DataType.Byte, DataType.identifyType("-129"));
        assertNotEquals(DataType.Byte, DataType.identifyType("128"));
    }

    @Test
    public void shouldIdentifyValidShort() {

        assertEquals(DataType.Short, DataType.identifyType("-32768"));
        assertEquals(DataType.Short, DataType.identifyType("32767"));

        assertNotEquals(DataType.Short, DataType.identifyType("-32769"));
        assertNotEquals(DataType.Short, DataType.identifyType("32768"));
    }

    @Test
    public void shouldIdentifyValidInt() {

        assertEquals(DataType.Int, DataType.identifyType(String.valueOf(Integer.MIN_VALUE)));
        assertEquals(DataType.Int, DataType.identifyType(String.valueOf(Integer.MAX_VALUE)));

        assertNotEquals(DataType.Int, DataType.identifyType(String.valueOf((long) Integer.MIN_VALUE - 1)));
        assertNotEquals(DataType.Int, DataType.identifyType(String.valueOf((long) Integer.MAX_VALUE + 1)));
    }

    @Test
    public void shouldIdentifyValidLong() {

        assertEquals(DataType.Long, DataType.identifyType(String.valueOf(Long.MIN_VALUE)));
        assertEquals(DataType.Long, DataType.identifyType(String.valueOf(Long.MAX_VALUE)));

        assertNotEquals(DataType.Long, DataType.identifyType(String.valueOf((double) Long.MIN_VALUE - 1)));
        assertNotEquals(DataType.Long, DataType.identifyType(String.valueOf((double) Long.MAX_VALUE + 1)));
    }

    @Test
    public void shouldIdentifyDoubleFromFloatValue() {

        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Float.MIN_VALUE)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Float.MAX_VALUE)));
    }

    @Test
    public void shouldIdentifyDoubleFromDoubleValue() {

        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Double.MAX_VALUE)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Double.MIN_VALUE)));
    }

    @Test
    public void shouldIdentifyDoubleFromDecimalValueThatRequiresRounding() {

        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Float.MIN_VALUE)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Float.MAX_VALUE)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Double.MAX_VALUE)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Double.MIN_VALUE)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Double.MIN_VALUE - 1)));
        assertEquals(DataType.Double, DataType.identifyType(String.valueOf(Double.MAX_VALUE + 1)));
    }

    @Test
    public void shouldIdentifyValidDate() {

        assertEquals(DataType.Date, DataType.identifyType("2015-06-24T12:50:35.556+01:00"));
        assertEquals(DataType.Date, DataType.identifyType("2015-07-04T19:32:24"));
        assertEquals(DataType.Date, DataType.identifyType("1984-10-11"));

        assertNotEquals(DataType.Date, DataType.identifyType("12:50:35.556+01:00"));
        assertNotEquals(DataType.Date, DataType.identifyType("P14DT16H12M"));
        assertNotEquals(DataType.Date, DataType.identifyType("01:42:19Z"));
    }

    @Test
    public void shouldReturnStringForBooleanAndNumber() {
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.Byte));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.Short));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.Int));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.Long));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.Double));

        assertEquals(DataType.String, DataType.getBroadestType(DataType.Byte, DataType.Boolean));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Short, DataType.Boolean));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Int, DataType.Boolean));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Long, DataType.Boolean));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Double, DataType.Boolean));

    }

    @Test
    public void shouldReturnStringForBooleanAndDate() {
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.Boolean));
    }

    @Test
    public void shouldReturnStringForDateAndNumber() {
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.Byte));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.Short));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.Int));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.Long));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.Double));

        assertEquals(DataType.String, DataType.getBroadestType(DataType.Byte, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Short, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Int, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Long, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Double, DataType.Date));
    }

    @Test
    public void shouldReturnStringForBooleanAndString() {
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Boolean));
    }

    @Test
    public void shouldReturnBroadestNumericTypeForNumbers() {
        assertEquals(DataType.Short, DataType.getBroadestType(DataType.Byte, DataType.Short));
        assertEquals(DataType.Int, DataType.getBroadestType(DataType.Byte, DataType.Int));
        assertEquals(DataType.Long, DataType.getBroadestType(DataType.Byte, DataType.Long));
        assertEquals(DataType.Double, DataType.getBroadestType(DataType.Byte, DataType.Double));

        assertEquals(DataType.Short, DataType.getBroadestType(DataType.Short, DataType.Byte));
        assertEquals(DataType.Int, DataType.getBroadestType(DataType.Int, DataType.Byte));
        assertEquals(DataType.Long, DataType.getBroadestType(DataType.Long, DataType.Byte));
        assertEquals(DataType.Double, DataType.getBroadestType(DataType.Double, DataType.Byte));
    }

    @Test
    public void shouldReturnNewTypeIfOldTypeIsNone() {
        assertEquals(DataType.Boolean, DataType.getBroadestType(DataType.None, DataType.Boolean));
        assertEquals(DataType.Byte, DataType.getBroadestType(DataType.None, DataType.Byte));
        assertEquals(DataType.Short, DataType.getBroadestType(DataType.None, DataType.Short));
        assertEquals(DataType.Int, DataType.getBroadestType(DataType.None, DataType.Int));
        assertEquals(DataType.Long, DataType.getBroadestType(DataType.None, DataType.Long));
        assertEquals(DataType.Double, DataType.getBroadestType(DataType.None, DataType.Double));
        assertEquals(DataType.Date, DataType.getBroadestType(DataType.None, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.None, DataType.String));
    }

    @Test
    public void shouldReturnOldTypeIfNewTypeIsNone() {
        assertEquals(DataType.Boolean, DataType.getBroadestType(DataType.Boolean, DataType.None));
        assertEquals(DataType.Byte, DataType.getBroadestType(DataType.Byte, DataType.None));
        assertEquals(DataType.Short, DataType.getBroadestType(DataType.Short, DataType.None));
        assertEquals(DataType.Int, DataType.getBroadestType(DataType.Int, DataType.None));
        assertEquals(DataType.Long, DataType.getBroadestType(DataType.Long, DataType.None));
        assertEquals(DataType.Double, DataType.getBroadestType(DataType.Double, DataType.None));
        assertEquals(DataType.Date, DataType.getBroadestType(DataType.Date, DataType.None));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.None));
    }

    @Test
    public void shouldReturnStringIfEitherOldOrNewTypeIsString() {

        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Boolean));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Byte));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Short));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Int));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Long));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Double));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.Date));

        assertEquals(DataType.String, DataType.getBroadestType(DataType.Boolean, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Byte, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Short, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Int, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Long, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Double, DataType.String));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.Date, DataType.String));
    }

    @Test
    public void shouldReturnSameTypeIfOldAndNewAreTheSame() {
        assertEquals(DataType.None, DataType.getBroadestType(DataType.None, DataType.None));
        assertEquals(DataType.Boolean, DataType.getBroadestType(DataType.Boolean, DataType.Boolean));
        assertEquals(DataType.Byte, DataType.getBroadestType(DataType.Byte, DataType.Byte));
        assertEquals(DataType.Short, DataType.getBroadestType(DataType.Short, DataType.Short));
        assertEquals(DataType.Int, DataType.getBroadestType(DataType.Int, DataType.Int));
        assertEquals(DataType.Long, DataType.getBroadestType(DataType.Long, DataType.Long));
        assertEquals(DataType.Double, DataType.getBroadestType(DataType.Double, DataType.Double));
        assertEquals(DataType.Date, DataType.getBroadestType(DataType.Date, DataType.Date));
        assertEquals(DataType.String, DataType.getBroadestType(DataType.String, DataType.String));
    }


}