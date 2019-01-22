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
