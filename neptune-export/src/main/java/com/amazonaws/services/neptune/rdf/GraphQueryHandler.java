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

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;

class GraphQueryHandler implements RDFHandler {

    private final RDFWriter writer;

    public GraphQueryHandler(RDFWriter writer) {
        this.writer = writer;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        writer.startRDF();
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        writer.endRDF();
    }

    @Override
    public void handleNamespace(String s, String s1) throws RDFHandlerException {

    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        writer.handleStatement(statement);
    }

    @Override
    public void handleComment(String s) throws RDFHandlerException {

    }
}
