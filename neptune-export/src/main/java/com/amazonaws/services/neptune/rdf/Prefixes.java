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

package com.amazonaws.services.neptune.rdf;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class Prefixes {

    private final Map<String, String> prefixes = new HashMap<>();
    private final int offset;

    public Prefixes() {
        prefixes.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
        prefixes.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
        prefixes.put("http://www.w3.org/2001/XMLSchema#", "xsd");

        offset = prefixes.size();
    }

    public String formatIRI(String s) {
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

    public void addTo(Path filePath) throws IOException {
        File source = filePath.toFile();
        LineIterator lineIterator = FileUtils.lineIterator(source);
        File tempFile = File.createTempFile(source.getName(), ".tmp");
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
        try {
            writer.write(allHeaders());
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
}
