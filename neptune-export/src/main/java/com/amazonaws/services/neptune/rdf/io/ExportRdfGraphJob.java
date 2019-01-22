package com.amazonaws.services.neptune.rdf.io;

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.rdf.NeptuneSparqlClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.GraphQueryResult;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ExportRdfGraphJob {

    private final Map<String, String> prefixes = new HashMap<>();
    private final int offset;
    private final NeptuneSparqlClient client;
    private final Directories directories;

    public ExportRdfGraphJob(NeptuneSparqlClient client, Directories directories) {
        this.client = client;
        this.directories = directories;

        prefixes.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
        prefixes.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
        prefixes.put("http://www.w3.org/2001/XMLSchema#", "xsd");

        offset = prefixes.size();
    }

    public void execute() throws IOException {

        System.err.println("Creating statement files");

        java.nio.file.Path filePath = directories.createFilePath(
                directories.statementsDirectory(), "statements", 0, () -> "ttl");

        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath.toFile()))) {

            Status status = new Status();
            String prevSubject = null;
            String prevPredicate = null;

            GraphQueryResult result = client.executeQuery("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");

            while (result.hasNext()) {
                Statement statement = result.next();

                String subject = formatIRI(statement.getSubject().stringValue());
                String predicate = formatIRI(statement.getPredicate().toString());

                if (prevSubject == null) {
                    writer.print(subject);
                    writer.print(" ");
                    writer.print(predicate);
                    writer.print(" ");
                } else if (prevSubject.equals(subject)) {
                    if (prevPredicate.equals(predicate)) {
                        writer.print(", ");
                    } else {
                        writer.print(" ;");
                        writer.print(System.lineSeparator());
                        writer.print(StringUtils.repeat(" ", subject.length() + 1));
                        writer.print(predicate);
                        writer.print(" ");
                    }
                } else {
                    writer.print(" .");
                    writer.print(System.lineSeparator());
                    writer.print(subject);
                    writer.print(" ");
                    writer.print(predicate);
                    writer.print(" ");
                }

                writer.print(formatObject(statement.getObject()));

                prevSubject = subject;
                prevPredicate = predicate;

                status.update();
            }

            writer.print(" .");
            writer.print(System.lineSeparator());
        }

        addHeaders(filePath.toFile(), allHeaders());
    }

    private String allHeaders() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : prefixes.entrySet()) {
            builder.append("@prefix ");
            builder.append(entry.getValue());
            builder.append(": <");
            builder.append(entry.getKey());
            builder.append("> .");
            builder.append(System.lineSeparator());
        }
        builder.append(System.lineSeparator());
        return builder.toString();
    }

    private String formatIRI(String s) {
        int i = s.indexOf("#");

        if (i > 0 && i < (s.length() - 1)) {
            String prefix = s.substring(0, i + 1);
            String value = s.substring(i + 1);

            if (!prefixes.containsKey(prefix)) {
                prefixes.put(prefix, "s" + (prefixes.size() - offset));
            }

            return String.format("%s:%s", prefixes.get(prefix), value);

        } else {
            return String.format("<%s>", s);
        }
    }

    private String formatObject(Value v) {

        String s1 = v.toString();
        String s2 = v.stringValue();

        if (s1.equals(s2)) {
            return formatIRI(s1);
        } else {
            return formatValue(s1);
        }
    }

    private String formatValue(String v) {
        if (v.contains("^^<http://www.w3.org/2001/XMLSchema#")) {
            String s = v.replace("^^<http://www.w3.org/2001/XMLSchema#", "^^xsd:");
            return s.substring(0, s.length() - 1);
        }
        return v;
    }

    private void addHeaders(File source, String headers) throws IOException {
        LineIterator lineIterator = FileUtils.lineIterator(source);
        File tempFile = File.createTempFile(source.getName(), ".tmp");
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
        try {
            writer.write(headers);
            while (lineIterator.hasNext()) {
                writer.write(lineIterator.next());
                writer.write(System.lineSeparator());
            }
        } finally {
            IOUtils.closeQuietly(writer);
            LineIterator.closeQuietly(lineIterator);
        }
        FileUtils.deleteQuietly(source);
        FileUtils.moveFile(tempFile, source);
    }
}
