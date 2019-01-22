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

package com.amazonaws.services.neptune.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public enum DirectoryStructure {
    Config {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(directory);
        }
    },
    PropertyGraph {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(nodesDirectory);
            Files.createDirectories(edgesDirectory);
        }
    },
    Rdf {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(statementsDirectory);
        }
    },
    GremlinQueries {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(resultsDirectory);
        }
    },
    SparqlQueries {
        @Override
        public void createDirectories(Path directory,
                                      Path nodesDirectory,
                                      Path edgesDirectory,
                                      Path statementsDirectory,
                                      Path resultsDirectory) throws IOException {
            Files.createDirectories(resultsDirectory);
        }
    };

    public abstract void createDirectories(Path directory,
                                           Path nodesDirectory,
                                           Path edgesDirectory,
                                           Path statementsDirectory,
                                           Path resultsDirectory) throws IOException;

}
