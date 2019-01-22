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
