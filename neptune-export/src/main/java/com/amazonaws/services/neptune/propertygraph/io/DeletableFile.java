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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

class DeletableFile implements AutoCloseable {

    private final File file;
    private boolean allowDelete = true;

    DeletableFile(File file) {
        this.file = file;
    }

    public Reader reader() throws FileNotFoundException {
        return new FileReader(file);
    }

    public String name() {
        return file.getName();
    }

    public void doNotDelete(){
        allowDelete = false;
    }

    @Override
    public void close() {
        if (file.exists() && allowDelete){
            boolean deletedOriginalFile = file.delete();

            if (!deletedOriginalFile) {
                throw new IllegalStateException("Unable to delete file: " + file.getAbsolutePath());
            }
        }
    }

    @Override
    public String toString() {
        return file.getAbsolutePath();
    }
}
