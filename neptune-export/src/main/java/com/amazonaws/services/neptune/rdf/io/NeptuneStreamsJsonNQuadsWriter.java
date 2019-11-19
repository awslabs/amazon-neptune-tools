package com.amazonaws.services.neptune.rdf.io;

import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.io.Status;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
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
            this.generator = new JsonFactory().
                    createGenerator(outputWriter.writer()).
                    setPrettyPrinter(new MinimalPrettyPrinter(System.lineSeparator()));
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
            outputWriter.start();

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
            generator.writeString(stringWriter.toString());

            generator.writeEndObject();

            generator.writeStringField("op", "ADD");
            generator.writeEndObject();

            outputWriter.finish();

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
