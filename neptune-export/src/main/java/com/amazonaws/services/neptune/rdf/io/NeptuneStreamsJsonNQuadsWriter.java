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

import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.io.Status;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.nquads.NQuadsWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

public class NeptuneStreamsJsonNQuadsWriter implements RDFWriter {

    private final JsonGenerator generator;
    private final Status status = new Status();
    private final OutputWriter outputWriter;

    public NeptuneStreamsJsonNQuadsWriter(OutputWriter outputWriter) {
        this.outputWriter = outputWriter;
        try {
            this.generator = new JsonFactory().createGenerator(outputWriter.writer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RDFFormat getRDFFormat() {
        return RDFFormat.NQUADS;
    }

    @Override
    public RDFWriter setWriterConfig(WriterConfig writerConfig) {
        throw new NotImplementedException();
    }

    @Override
    public WriterConfig getWriterConfig() {
        throw new NotImplementedException();
    }

    @Override
    public Collection<RioSetting<?>> getSupportedSettings() {
        throw new NotImplementedException();
    }

    @Override
    public <T> RDFWriter set(RioSetting<T> rioSetting, T t) {
        throw new NotImplementedException();
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        // Do nothing
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        // Do nothing
    }

    @Override
    public void handleNamespace(String s, String s1) throws RDFHandlerException {
        // Do nothing
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {

        try {
            outputWriter.startCommit();

            generator.writeStartObject();

            generator.writeObjectFieldStart("eventId");
            generator.writeNumberField("commitNum", -1);
            generator.writeNumberField("opNum", 0);
            generator.writeEndObject();

            generator.writeObjectFieldStart("data");

            generator.writeFieldName("stmt");

            StringWriter stringWriter = new StringWriter();
            NQuadsWriter nQuadsWriter = new NQuadsWriter(stringWriter);
            nQuadsWriter.startRDF();
            nQuadsWriter.handleStatement(statement);
            nQuadsWriter.endRDF();
            generator.writeString(stringWriter.toString().replace(System.lineSeparator(), ""));

            generator.writeEndObject();

            generator.writeStringField("op", "ADD");
            generator.writeEndObject();
            generator.flush();

            outputWriter.endCommit();

            status.update();


        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }

    @Override
    public void handleComment(String s) throws RDFHandlerException {
        // Do nothing
    }
}
