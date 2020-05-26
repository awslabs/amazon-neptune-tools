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

package com.amazonaws.services.neptune.export;

import org.junit.Test;

import static org.junit.Assert.*;

public class ArgsTest {

    @Test
    public void shouldRemoveOptions() throws Exception {

        Args args = new Args("-x \"extra\" -e endpoint -c config -q query -f file");
        args.removeOptions("-x", "-c", "-f", "-notpresent");

        assertArrayEquals(new String[]{"-e", "endpoint", "-q", "query"}, args.values());
    }

    @Test
    public void shouldRemoveMultipleOccurrencesOfOption() throws Exception {

        Args args = new Args("-e endpoint -l label1 -l label2 -l label3");
        args.removeOptions("-l");

        assertArrayEquals(new String[]{"-e", "endpoint"}, args.values());
    }

    @Test
    public void shouldRemoveFlags() throws Exception {

        Args args = new Args("-e endpoint -flag1 -c config -flag2 -q query");
        args.removeFlags("-flag1", "-flag2");

        assertArrayEquals(new String[]{"-e", "endpoint", "-c", "config", "-q", "query"}, args.values());
    }

    @Test
    public void shouldAddOption() throws Exception {
        Args args = new Args("-e endpoint -c config");
        args.addOption("-l", "label1");
        args.addOption("-q", "result=\"g.V('id').toList()\"");

        assertArrayEquals(new String[]{"-e", "endpoint", "-c", "config", "-l", "label1", "-q", "result=\"g.V('id').toList()\""}, args.values());
    }

    @Test
    public void shouldFormatAsString() throws Exception {
        Args args = new Args("-e endpoint -c config");
        assertEquals("-e endpoint -c config", args.toString());
    }

}