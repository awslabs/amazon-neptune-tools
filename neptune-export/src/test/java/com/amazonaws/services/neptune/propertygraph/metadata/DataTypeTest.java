package com.amazonaws.services.neptune.propertygraph.metadata;

import org.junit.Test;

import static org.junit.Assert.*;

public class DataTypeTest {

    @Test
    public void emptyStringDateValueShouldReturnEmptyString(){
        String result = DataType.Date.format("");
        assertEquals("", result);
    }

}