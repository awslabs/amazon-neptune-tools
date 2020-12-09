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

import static org.junit.Assert.*;

public class DirectoriesTest {

    @Test
    public void replacesForbiddenCharactersInFilename(){
        String filename = "(Person;Staff;Temp\\;Holidays)-works_for-(Admin;Perm;Person)";
        String updated = Directories.fileName(filename, 1);
        assertEquals("(Person_Staff_Temp__Holidays)-works_for-(Admin_Perm_Person)-1", updated);
    }

}