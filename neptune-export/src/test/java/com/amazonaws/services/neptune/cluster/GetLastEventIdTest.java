package com.amazonaws.services.neptune.cluster;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class GetLastEventIdTest {

    @Test
    public void shouldReturnIntegerMaxValueForEngineVersions1041AndBelow(){

        String expectedValue = String.valueOf(Integer.MAX_VALUE);

        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.1.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.1.1"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.1.2"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.2.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.2.1"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.2.2"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.3.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.4.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.4.1"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.4.2"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.5.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.5.1"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.1.0.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.1.1.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.2.0.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.2.0.1"));

    }

    @Test
    public void shouldReturnLongMaxValueForEngineVersions1041AndBelow(){

        String expectedValue = String.valueOf(Long.MAX_VALUE);

        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.1.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.1.1"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.1.2"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.2.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.2.1"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.2.2"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.3.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.4.0"));
        Assert.assertNotEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.4.1"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.4.2"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.5.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.0.5.1"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.1.0.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.1.1.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.2.0.0"));
        Assert.assertEquals(expectedValue, GetLastEventId.MaxCommitNumValueForEngine("1.2.0.1"));

    }

}