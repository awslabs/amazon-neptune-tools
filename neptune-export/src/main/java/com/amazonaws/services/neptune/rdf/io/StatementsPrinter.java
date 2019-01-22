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

package com.amazonaws.services.neptune.rdf.io;

import org.apache.commons.lang.StringUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

public class StatementsPrinter implements AutoCloseable {
    private final PrintWriter writer;

    public StatementsPrinter(Path filePath) throws IOException {
        this.writer = new PrintWriter(new FileWriter(filePath.toFile()));
    }

    public void printObject(String o) {
        writer.print(o);
    }

    public void printTriple(String subject, String predicate) {
        writer.print(" .");
        writer.print(System.lineSeparator());
        writer.print(subject);
        writer.print(" ");
        writer.print(predicate);
        writer.print(" ");
    }

    public void printFirstLine(String subject, String predicate) {
        writer.print(subject);
        writer.print(" ");
        writer.print(predicate);
        writer.print(" ");
    }

    public void printObjectList() {
        writer.print(", ");
    }

    public void printPredicateList(String subject, String predicate) {
        writer.print(" ;");
        writer.print(System.lineSeparator());
        writer.print(StringUtils.repeat(" ", subject.length() + 1));
        writer.print(predicate);
        writer.print(" ");
    }

    @Override
    public void close() throws Exception {
        writer.print(" .");
        writer.print(System.lineSeparator());
        writer.close();
    }
}
