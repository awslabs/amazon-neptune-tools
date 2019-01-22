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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.rdf.NeptuneSparqlClient;
import com.amazonaws.services.neptune.rdf.Prefixes;
import com.amazonaws.services.neptune.rdf.StatementHandler;
import org.eclipse.rdf4j.query.GraphQueryResult;

public class ExportRdfGraphJob {

    private final Prefixes prefixes = new Prefixes();
    private final NeptuneSparqlClient client;
    private final Directories directories;

    public ExportRdfGraphJob(NeptuneSparqlClient client, Directories directories) {
        this.client = client;
        this.directories = directories;
    }

    public void execute() throws Exception {

        System.err.println("Creating statement files");

        java.nio.file.Path filePath = directories.createFilePath(
                directories.statementsDirectory(), "statements", 0, () -> "ttl");

        try (StatementsPrinter printer = new StatementsPrinter(filePath)) {

            Status status = new Status();
            StatementHandler handler = StatementHandler.create(prefixes);

            GraphQueryResult result = client.executeQuery("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");

            while (result.hasNext()) {

                handler = handler.handle(result.next());
                handler.printTo(printer);

                status.update();
            }
        }

        prefixes.addTo(filePath);
    }
}
