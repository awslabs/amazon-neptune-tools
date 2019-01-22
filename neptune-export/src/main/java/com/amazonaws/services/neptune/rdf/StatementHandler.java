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

import com.amazonaws.services.neptune.rdf.io.StatementsPrinter;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

public class StatementHandler {

    public static StatementHandler create(Prefixes prefixes){
        return new StatementHandler(prefixes, null, null, null, null, null);
    }

    private final Prefixes prefixes;
    private final String prevSubject;
    private final String prevPredicate;
    private final String subject;
    private final String predicate;
    private final String object;

    private StatementHandler(Prefixes prefixes,
                             String prevSubject,
                             String prevPredicate,
                             String subject,
                             String predicate,
                             String object) {
        this.prefixes = prefixes;
        this.prevSubject = prevSubject;
        this.prevPredicate = prevPredicate;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public StatementHandler handle(Statement statement){
        return new StatementHandler(
                prefixes, subject,
                predicate,
                prefixes.formatIRI(statement.getSubject().stringValue()),
                prefixes.formatIRI(statement.getPredicate().toString()),
                formatObject(statement.getObject()));
    }

    public void printTo(StatementsPrinter printer){
        if (subject == null || predicate == null || object == null){
            throw new IllegalStateException("Statement handler has not been initialized with a statement");
        }

        if (prevSubject == null) {
            printer.printFirstLine(subject, predicate);
        } else if (prevSubject.equals(subject)) {
            if (prevPredicate.equals(predicate)) {
                printer.printObjectList();
            } else {
                printer.printPredicateList(subject, predicate);
            }
        } else {
            printer.printTriple(subject, predicate);
        }

        printer.printObject(object);
    }

    private String formatObject(Value v) {

        String s1 = v.toString();
        String s2 = v.stringValue();

        if (s1.equals(s2)) {
            return prefixes.formatIRI(s1);
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
}
