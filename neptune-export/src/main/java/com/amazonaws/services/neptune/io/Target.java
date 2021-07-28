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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

public enum Target implements CommandWriter {

    files {
        @Override
        public OutputWriter createOutputWriter(Supplier<Path> pathSupplier, KinesisConfig kinesisConfig) throws IOException {
            File file = pathSupplier.get().toFile();
            boolean isNewTarget = !(file.exists());
            return new PrintOutputWriter(file.getAbsolutePath(), isNewTarget, new FileWriter(file));
        }

        @Override
        public void writeReturnValue(String value) {
            System.out.println(value);
        }
    },
    stdout {
        @Override
        public OutputWriter createOutputWriter(Supplier<Path> pathSupplier, KinesisConfig kinesisConfig) throws IOException {
            return new StdOutPrintOutputWriter();
        }

        @Override
        public void writeReturnValue(String value) {
            System.err.println(value);
        }
    },
    stream {
        @Override
        public OutputWriter createOutputWriter(Supplier<Path> pathSupplier, KinesisConfig kinesisConfig) throws IOException {

            Path filePath = pathSupplier.get();
            File file = filePath.toFile();

            return new FileToStreamOutputWriter(
                    new KinesisStreamPrintOutputWriter(file.getAbsolutePath(), new FileWriter(file)),
                    filePath,
                    kinesisConfig);
        }

        @Override
        public void writeReturnValue(String value) {
            System.out.println(value);
        }
    };

    @Override
    public void writeMessage(String value) {
        System.err.println(value);
    }

    public abstract OutputWriter createOutputWriter(Supplier<Path> pathSupplier, KinesisConfig kinesisConfig) throws IOException;

    @Override
    public abstract void writeReturnValue(String value);

}
