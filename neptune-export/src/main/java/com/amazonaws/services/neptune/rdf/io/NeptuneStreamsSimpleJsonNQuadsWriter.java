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
import com.amazonaws.services.neptune.io.StatusOutputFormat;
import com.amazonaws.services.neptune.util.NotImplementedException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.eclipse.rdf4j.common.text.ASCIIUtil;
import org.eclipse.rdf4j.common.text.StringUtil;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import org.eclipse.rdf4j.rio.nquads.NQuadsWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

public class NeptuneStreamsSimpleJsonNQuadsWriter implements RDFWriter {

    private static final String REGEX_LAST_NEWLINE = String.format("%s$", System.lineSeparator());

    private final JsonGenerator generator;
    private final Status status = new Status(StatusOutputFormat.Description, "records");
    private final OutputWriter outputWriter;

    public NeptuneStreamsSimpleJsonNQuadsWriter(OutputWriter outputWriter) {
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

            generator.writeStringField("id", "");
            generator.writeStringField("from", "");
            generator.writeStringField("to", "");

            generator.writeStringField("type", "");
            generator.writeStringField("key", "");
            generator.writeStringField("value", "");
            generator.writeStringField("dataType", "");

//            generator.writeStringField("s", getValue(statement.getSubject()));
//            generator.writeStringField("p", getValue(statement.getPredicate()));
//            generator.writeStringField("o", getValue(statement.getObject()));
//
//            if (statement.getContext() != null) {
//                generator.writeStringField("g", getValue(statement.getContext()));
//            } else {
//                generator.writeStringField("g", "");
//            }

            generator.writeStringField("s", "");
            generator.writeStringField("p", "");
            generator.writeStringField("o", "");
            generator.writeStringField("g", "");

            generator.writeFieldName("stmt");

            StringWriter stringWriter = new StringWriter();
            NQuadsWriter nQuadsWriter = new NQuadsWriter(stringWriter);
            nQuadsWriter.startRDF();
            nQuadsWriter.handleStatement(statement);
            nQuadsWriter.endRDF();
            generator.writeString(stringWriter.toString().replaceAll(REGEX_LAST_NEWLINE, ""));

            generator.writeStringField("op", "ADD");
            generator.writeEndObject();
            generator.writeRaw(outputWriter.lineSeparator());
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

    private String getValue(Value value) throws IOException {
        if (value instanceof IRI) {
            return getIRI((IRI) value);
        } else if (value instanceof BNode) {
            return getBNode((BNode) value);
        } else {
            if (!(value instanceof Literal)) {
                throw new IllegalArgumentException("Unknown value type: " + value.getClass());
            }

            return getLiteral((Literal) value);
        }

    }

    private String getIRI(IRI iri) throws IOException {
        StringWriter appendable = new StringWriter();
        StringUtil.simpleEscapeIRI(iri.toString(), appendable, true);
        return appendable.toString();
    }

    private String getBNode(BNode bNode) throws IOException {
        StringWriter appendable = new StringWriter();

        String nextId = bNode.getID();
        appendable.append("_:");
        if (nextId.isEmpty()) {
            appendable.append("genid");
            appendable.append(Integer.toHexString(bNode.hashCode()));
        } else {
            if (!ASCIIUtil.isLetter(nextId.charAt(0))) {
                appendable.append("genid");
                appendable.append(Integer.toHexString(nextId.charAt(0)));
            }

            for (int i = 0; i < nextId.length(); ++i) {
                if (ASCIIUtil.isLetterOrNumber(nextId.charAt(i))) {
                    appendable.append(nextId.charAt(i));
                } else {
                    appendable.append(Integer.toHexString(nextId.charAt(i)));
                }
            }
        }

        return appendable.toString();

    }

    private String getLiteral(Literal lit) throws IOException {
        StringWriter appendable = new StringWriter();
        NTriplesUtil.append(lit, appendable, true, true);
        return appendable.toString();
    }


}
