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

package com.amazonaws.services.neptune.propertygraph.io;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class RenameableFiles {

    private final Map<File, String> entries = new HashMap<>();

    public void add(File file, String filename) {
        entries.put(file, filename);
    }

    public Collection<File> rename() {
        Collection<File> renamedFiles = new ArrayList<>();
        for (Map.Entry<File, String> entry : entries.entrySet()) {
            File file = entry.getKey();
            File renamedFile = new File(file.getParentFile(), entry.getValue());
            file.renameTo(renamedFile);
            renamedFiles.add(renamedFile);
        }
        return renamedFiles;
    }
}
