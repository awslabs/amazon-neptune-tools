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

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class FileToStreamOutputWriter implements OutputWriter {


    private final OutputWriter innerOutputWriter;
    private final Path filePath;
    private final Stream stream;
    private final Tailer tailer;
    private final ExportListener listener;

    FileToStreamOutputWriter(OutputWriter innerOutputWriter, Path filePath, KinesisConfig kinesisConfig) {
        this.innerOutputWriter = innerOutputWriter;
        this.filePath = filePath;
        this.stream = kinesisConfig.stream();
        this.listener = new ExportListener(stream);
        this.tailer = Tailer.create(filePath.toFile(), listener);
    }

    @Override
    public void startCommit() {
        innerOutputWriter.startCommit();
    }

    @Override
    public void endCommit() {
        innerOutputWriter.endCommit();
        listener.incrementTotalLineCount();
    }

    @Override
    public void print(String s) {
        innerOutputWriter.print(s);
    }

    @Override
    public Writer writer() {
        return innerOutputWriter.writer();
    }

    @Override
    public void startOp() {
        innerOutputWriter.startOp();
    }

    @Override
    public void endOp() {
        innerOutputWriter.endOp();
    }

    @Override
    public void close() throws Exception {
        innerOutputWriter.close();
        while (!listener.isFinished()) {
            Thread.sleep(1000);
        }
        tailer.stop();
        stream.flushRecords();
        Files.deleteIfExists(filePath);
    }

    private static class ExportListener extends TailerListenerAdapter {

        private final Stream stream;
        private final AtomicInteger totalLineCount = new AtomicInteger(0);
        private int linesProcessed = 0;

        private ExportListener(Stream stream) {
            this.stream = stream;
        }

        public void handle(String line) {
            stream.publish(line);
            linesProcessed++;
        }

        public void incrementTotalLineCount() {
            totalLineCount.incrementAndGet();
        }

        public boolean isFinished() {
            return linesProcessed == totalLineCount.get();
        }
    }

}
