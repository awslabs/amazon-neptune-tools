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

package com.amazonaws.services.neptune.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

public class CSVUtils {

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withEscape('\\').withQuoteMode(QuoteMode.NONE);

    public static CSVParser newParser(File file) throws IOException {
        return newParser(file.toPath());
    }

    public static CSVParser newParser(Path filePath) throws IOException {
        return CSVParser.parse(filePath, Charset.forName("UTF-8"), CSVFormat.DEFAULT);
    }

    public static CSVRecord firstRecord(String s) {
        try {
            CSVParser parser = CSVParser.parse(s, CSVFormat.DEFAULT);
            List<CSVRecord> records = parser.getRecords();
            if (records.size() < 1) {
                throw new IllegalArgumentException("Unable to find first record: " + s);
            }
            return records.get(0);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to find first record: " + s);
        }
    }
}
