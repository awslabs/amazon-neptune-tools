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

import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

public class RawCsvPrinter implements Flushable, AutoCloseable {

    static RawCsvPrinter newPrinter(Path filePath, boolean append) throws IOException {
        return new RawCsvPrinter(filePath, append);
    }

    static RawCsvPrinter newPrinter(Path filePath) throws IOException {
        return newPrinter(filePath, false);
    }

    private final PrintWriter printer;

    private RawCsvPrinter(Path filePath, boolean append) throws IOException {
        this.printer = new PrintWriter(new FileWriter(filePath.toFile(), append));
    }

    void printRecord(Iterable<String> values){
        printer.write(String.join(",", values));
        printer.write(System.lineSeparator());
    }

    void printRecord(String value){
        printer.write(value);
        printer.write(System.lineSeparator());
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    @Override
    public void close() throws IOException {
        printer.close();
    }
}
