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

package com.amazonaws.services.neptune.io;

import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

public class DirectoriesTest {

    @Test
    public void replacesForbiddenCharactersInFilename() throws UnsupportedEncodingException {
        String filename = "(Person;Staff;Temp\\;Holidays)-works_for-(Admin;Perm;Person)";
        String updated = Directories.fileName(filename, 1);
        assertEquals("%28Person%3BStaff%3BTemp%5C%3BHolidays%29-works_for-%28Admin%3BPerm%3BPerson%29-1", updated);
    }

}