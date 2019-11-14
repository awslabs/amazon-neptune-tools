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

package com.amazonaws.services.neptune.propertygraph.io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

public enum Output {
    files {
        @Override
        public PrintWriter createPrintWriter(Path filePath) throws IOException {
            return new PrintWriter(new FileWriter(filePath.toFile()));
        }

        @Override
        public void writeCommandResult(Object result) {
            System.out.println(result);
        }
    },
    stdout {
        @Override
        public PrintWriter createPrintWriter(Path filePath) {
            return new PrintWriter(System.out);
        }

        @Override
        public void writeCommandResult(Object result) {
            System.err.println(result);
        }
    };

    public abstract PrintWriter createPrintWriter(Path filePath) throws IOException;

    public abstract void writeCommandResult(Object result);

}
