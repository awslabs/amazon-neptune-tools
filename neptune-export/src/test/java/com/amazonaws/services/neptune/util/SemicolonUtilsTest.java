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

package com.amazonaws.services.neptune.util;

import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

public class SemicolonUtilsTest {

    @Test
    public void shouldSplitStringOnSemicolons(){

        Collection<String> results = SemicolonUtils.split("abc;def;ghi");

        assertEquals(3, results.size());

        Iterator<String> iterator = results.iterator();

        assertEquals("abc", iterator.next());
        assertEquals("def", iterator.next());
        assertEquals("ghi", iterator.next());
    }

    @Test
    public void shouldNotSplitOnEscapedSemicolon(){

        Collection<String> results = SemicolonUtils.split("abc;d\\;ef;ghi");

        assertEquals(3, results.size());

        Iterator<String> iterator = results.iterator();

        assertEquals("abc", iterator.next());
        assertEquals("d\\;ef", iterator.next());
        assertEquals("ghi", iterator.next());
    }

    @Test
    public void shouldUnescapeEscapedSemicolonIfThereAreNoUnescapedSemicolonsInString(){
       assertEquals("d;ef",  SemicolonUtils.unescape("d\\;ef"));
    }

    @Test
    public void shouldReturnEmptyCollectionForEmptyString(){
        Collection<String> collection = SemicolonUtils.split("");
        assertEquals(0, collection.size());
    }

}