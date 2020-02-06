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

package com.amazonaws.services.neptune.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Directories {

    public static Directories createFor(File root) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("You must supply a directory");
        }

        String directoryName = String.valueOf(System.currentTimeMillis());
        Path rootDirectory = root.toPath();

        Path directory = rootDirectory.resolve(directoryName);
        Files.createDirectories(directory);

        return new Directories(directory);
    }

    private final Path directory;

    private Directories(Path directory) {
        this.directory = directory;
    }

    public Path outputDirectory() {
        return directory;
    }

    Path createFilePath(String name) {
        return createFilePath(name, null);
    }

    Path createFilePath(String name, Object index) {

        String filename = index == null ?
                String.format("%s.csv", name) :
                String.format("%s-%s.csv", name, index);

        return directory.resolve(filename);
    }
}

