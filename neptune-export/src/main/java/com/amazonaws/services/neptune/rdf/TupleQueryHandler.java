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

import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFWriter;

import java.util.List;

class TupleQueryHandler implements TupleQueryResultHandler {

    private final RDFWriter writer;
    private final ValueFactory factory;

    public TupleQueryHandler(RDFWriter writer, ValueFactory factory) {
        this.writer = writer;
        this.factory = factory;
    }

    @Override
    public void handleBoolean(boolean value) throws QueryResultHandlerException {

    }

    @Override
    public void handleLinks(List<String> linkUrls) throws QueryResultHandlerException {

    }

    @Override
    public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
        writer.startRDF();
    }

    @Override
    public void endQueryResult() throws TupleQueryResultHandlerException {
        writer.endRDF();
    }

    @Override
    public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
        Value s = bindingSet.getValue("s");
        Value p = bindingSet.getValue("p");
        Value o = bindingSet.getValue("o");
        Value g = bindingSet.getValue("g");

        if (s == null || p == null || o == null || g == null){
            throw new IllegalArgumentException("SPARQL query must return results with s, p, o and g values. For example: SELECT * FROM NAMED <http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph> WHERE { GRAPH ?g {?s a <http://kelvinlawrence.net/air-routes/class/Airport>. ?s ?p ?o}} LIMIT 10");
        }


        Resource subject = s.isIRI() ? factory.createIRI(s.stringValue()) : factory.createBNode(s.stringValue());
        IRI predicate = factory.createIRI(p.stringValue());
        IRI graph = getNonDefaultNamedGraph(g, factory);

        Statement statement = factory.createStatement(subject, predicate, o, graph);

        writer.handleStatement(statement);
    }

    private IRI getNonDefaultNamedGraph(Value g, ValueFactory factory) {
        String s = g.stringValue();

        if (StringUtils.isEmpty(s) || s.equalsIgnoreCase("http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph")) {
            return null;
        }

        return factory.createIRI(s);
    }

}
