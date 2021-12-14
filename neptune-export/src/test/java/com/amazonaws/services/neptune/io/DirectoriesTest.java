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

import com.amazonaws.services.neptune.propertygraph.io.PropertyGraphExportFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DirectoriesTest {

    @Test
    public void replacesForbiddenCharactersInFilename() throws UnsupportedEncodingException {
        String filename = "(Person;Staff;Temp\\;Holidays)-works_for-(Admin;Perm;Person)";
        String updated = Directories.fileName(filename, new AtomicInteger());
        assertEquals("%28Person%3BStaff%3BTemp%5C%3BHolidays%29-works_for-%28Admin%3BPerm%3BPerson%29-1", updated);
    }

    @Test
    public void createsDigestFilePathsForVeryLongFilenames() throws IOException {
        Path path = Paths.get("/export");
        String longName = "abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz1234567890";
        Directories directories = Directories.createFor(DirectoryStructure.PropertyGraph, new File("home"), "export-id", "", "");
        Path filePath = directories.createFilePath(path, longName, PropertyGraphExportFormat.csv);

        assertEquals("/export/8044f12c352773b7ff400ef524da6e90db419e4a.csv", filePath.toString());
    }


}