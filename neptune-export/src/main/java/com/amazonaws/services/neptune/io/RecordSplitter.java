/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory;
import org.eclipse.rdf4j.rio.nquads.NQuadsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class RecordSplitter {

    private static final Logger logger = LoggerFactory.getLogger(RecordSplitter.class);

    public static Collection<String> splitByLength(String s, int length){
        return splitByLength(s ,length, 10);
    }

    public static Collection<String> splitByLength(String s, int length, int wordBoundaryMargin) {

        int startIndex = 0;

        Collection<String> results = new ArrayList<>();

        while (startIndex < s.length()) {

            boolean foundWordBoundary = false;

            int endIndex = Math.min(startIndex + length, s.length());
            int minCandidateEndIndex = Math.max(startIndex +1, endIndex - wordBoundaryMargin);

            for (int actualEndIndex = endIndex; actualEndIndex >= minCandidateEndIndex; actualEndIndex--){

                if (!StringUtils.isAlphanumeric( s.substring(actualEndIndex - 1, actualEndIndex))){

                    String result = s.substring(startIndex, actualEndIndex);
                    String trimmedResult = result.trim();
                    if (StringUtils.isNotEmpty(trimmedResult)){
                        results.add(trimmedResult);
                    }
                    startIndex = actualEndIndex;
                    foundWordBoundary = true;
                    break;
                }
            }

            if (!foundWordBoundary){
                String result = s.substring(startIndex, endIndex);
                String trimmedResult = result.trim();
                if (StringUtils.isNotEmpty(trimmedResult)){
                    results.add(trimmedResult);
                }
                startIndex = endIndex;
            }

        }

        return results;

    }

    private static int calculateStringMaxLength(int maxLength, int recordLength, int valueLength) {
        return maxLength - (recordLength - valueLength) - 2;
    }

    private final int maxSize;
    private final LargeStreamRecordHandlingStrategy largeStreamRecordHandlingStrategy;
    private final ObjectMapper mapper = new ObjectMapper();
    private final RDFParser parser = new NQuadsParserFactory().getParser();
    private final StatementHandler handler = new StatementHandler();

    public RecordSplitter(int maxSize, LargeStreamRecordHandlingStrategy largeStreamRecordHandlingStrategy) {
        this.maxSize = maxSize;
        this.largeStreamRecordHandlingStrategy = largeStreamRecordHandlingStrategy;
        this.parser.setRDFHandler(handler);
    }

    public Collection<String> split(String s) {
        Collection<String> results = new ArrayList<>();
        int opNum = 1;
        try {
            JsonNode json = mapper.readTree(s);
            for (JsonNode jsonNode : json) {
                if (isNeptuneStreamEvent(jsonNode)) {
                    Collection<String> events = splitNeptuneStreamEvent(jsonNode, opNum);
                    results.addAll(events);
                    opNum += events.size();
                } else {
                    JsonNodeType nodeType = jsonNode.getNodeType();
                    if (nodeType == JsonNodeType.NUMBER) {
                        results.addAll(splitNumber(jsonNode));
                    } else if (nodeType == JsonNodeType.STRING) {
                        results.addAll(splitString(jsonNode));
                    } else {
                        // This may end up being dropped
                        results.add(format(jsonNode.toString()));
                    }
                }
            }
        } catch (JsonProcessingException e) {
            // This will almost certainly be dropped
            results.add(s);
        }
        return results;
    }

    private Collection<String> splitNeptuneStreamEvent(JsonNode jsonNode, int opNum) {
        Collection<String> results = new ArrayList<>();
        ((ObjectNode) jsonNode.get("eventId")).replace("opNum", mapper.valueToTree(opNum));
        String jsonString = jsonNode.toString();
        int eventJsonLength = jsonString.length();
        if (eventJsonLength > maxSize && largeStreamRecordHandlingStrategy.allowShred()) {
            if (isProperytGraphEvent(jsonNode)) {
                String value = jsonNode.get("data").get("value").get("value").textValue();
                int maxStringLength = calculateStringMaxLength(maxSize, eventJsonLength, value.length());
                Collection<String> splitValues = splitByLength(value, maxStringLength);
                for (String splitValue : splitValues) {
                    ((ObjectNode) jsonNode.get("eventId")).replace("opNum", mapper.valueToTree(opNum));
                    ((ObjectNode) jsonNode.get("data").get("value")).replace("value", mapper.valueToTree(splitValue));
                    results.add(format(jsonNode.toString()));
                    opNum += 1;
                }
            } else {
                String statement = jsonNode.get("data").get("stmt").textValue();
                int statementLength = statement.length();
                int maxStatementLength = calculateStringMaxLength(maxSize, eventJsonLength, statementLength);
                handler.reset(statementLength, maxStatementLength);
                try {
                    parser.parse(new StringReader(statement));
                    for (String splitStatement : handler.statements()) {
                        ((ObjectNode) jsonNode.get("eventId")).replace("opNum", mapper.valueToTree(opNum));
                        ((ObjectNode) jsonNode.get("data")).replace("stmt", mapper.valueToTree(splitStatement));
                        results.add(format(jsonNode.toString()));
                        opNum += 1;
                    }
                } catch (IOException e) {
                    // What to do here?
                    results.add(format(jsonString));
                }
            }
        } else {
            results.add(format(jsonString));
        }

        return results;
    }

    private boolean isProperytGraphEvent(JsonNode jsonNode) {
        return jsonNode.get("data").has("value");
    }

    private Collection<String> splitString(JsonNode jsonNode) {
        Collection<String> results = new ArrayList<>();
        String jsonString = jsonNode.textValue();
        if (jsonString.length() > maxSize) {
            Collection<String> splitValues = splitByLength(jsonString, maxSize);
            for (String splitValue : splitValues) {
                results.add(format(splitValue, true));
            }
        } else {
            results.add(format(jsonString, true));
        }
        return results;
    }

    private Collection<String> splitNumber(JsonNode jsonNode) {
        return Collections.singletonList(format(jsonNode.asText()));
    }

    private boolean isNeptuneStreamEvent(JsonNode jsonNode) {
        return jsonNode.has("eventId");
    }

    private String format(String s) {
        return format(s, false);
    }

    private String format(String s, boolean addQuotes) {
        if (addQuotes) {
            return String.format("[\"%s\"]", s);
        } else {
            return String.format("[%s]", s);
        }
    }

    private static class StatementHandler implements RDFHandler {

        private final Collection<String> results = new ArrayList<>();
        private int statementLength;
        private int maxStatementLength;

        @Override
        public void startRDF() throws RDFHandlerException {

        }

        @Override
        public void endRDF() throws RDFHandlerException {

        }

        @Override
        public void handleNamespace(String s, String s1) throws RDFHandlerException {

        }

        @Override
        public void handleStatement(Statement statement) throws RDFHandlerException {
            Value object = statement.getObject();
            if (object.isLiteral()) {
                String objectValue = object.stringValue();
                int maxObjectLength = calculateStringMaxLength(maxStatementLength, statementLength, objectValue.length());
                Collection<String> splitValues = splitByLength(objectValue, maxObjectLength);
                for (String splitValue : splitValues) {
                    StringWriter writer = new StringWriter();
                    new NQuadsWriter(writer).consumeStatement(new Statement() {
                        @Override
                        public Resource getSubject() {
                            return statement.getSubject();
                        }

                        @Override
                        public IRI getPredicate() {
                            return statement.getPredicate();
                        }

                        @Override
                        public Value getObject() {
                            return SimpleValueFactory.getInstance().createLiteral(splitValue);
                        }

                        @Override
                        public Resource getContext() {
                            return statement.getContext();
                        }
                    });
                    results.add(writer.toString());
                }
            } else {
                results.add(String.format("%s\n", statement.toString()));
            }
        }

        @Override
        public void handleComment(String s) throws RDFHandlerException {

        }

        public void reset(int statementLength, int maxStatementLength) {
            this.statementLength = statementLength;
            this.maxStatementLength = maxStatementLength;
            results.clear();
        }

        public Collection<String> statements() {
            return results;
        }
    }
}
