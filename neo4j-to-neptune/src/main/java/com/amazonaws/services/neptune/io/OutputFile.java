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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class OutputFile implements AutoCloseable {

    private RawCsvPrinter printer;

    private final Directories directories;
    private final String filename;

    public OutputFile(Directories directories, String filename) throws IOException {

        this.directories = directories;
        this.filename = filename;

        this.printer = RawCsvPrinter.newPrinter(directories.createFilePath(filename));
    }

    public void printRecord(Iterable<String> values) throws IOException {
        printer.printRecord(values);
    }

    public void printHeaders(Iterable<String> headers) throws IOException {

        Path originalFilePath = directories.createFilePath(filename);
        Path tempFilePath = directories.createFilePath(filename, "temp");

        printer.flush();
        printer.close();

        try (Stream<String> stream = Files.lines(originalFilePath);
             RawCsvPrinter tempFilePrinter = RawCsvPrinter.newPrinter(tempFilePath)) {

            tempFilePrinter.printRecord(headers);
            stream.forEach(tempFilePrinter::printRecord);

            tempFilePrinter.flush();
        }

        Files.deleteIfExists(originalFilePath);
        if (!tempFilePath.toFile().renameTo(originalFilePath.toFile())) {
            throw new RuntimeException("Unable to rename temp file: " + tempFilePath);
        }

        printer = RawCsvPrinter.newPrinter(originalFilePath, true);
    }

    @Override
    public void close() throws Exception {
        printer.flush();
        printer.close();
    }
}
