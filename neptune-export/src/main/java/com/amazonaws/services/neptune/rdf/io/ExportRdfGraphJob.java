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

import com.amazonaws.services.neptune.rdf.NeptuneSparqlClient;
import com.amazonaws.services.neptune.util.Timer;

public class ExportRdfGraphJob {

    private final NeptuneSparqlClient client;
    private final RdfTargetConfig targetConfig;

    public ExportRdfGraphJob(NeptuneSparqlClient client, RdfTargetConfig targetConfig) {
        this.client = client;
        this.targetConfig = targetConfig;
    }

    public void execute() throws Exception {

        try (Timer timer = new Timer("exporting RDF as " + targetConfig.format().description())) {
            System.err.println("Creating statement files");

            client.executeQuery("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }", targetConfig);
        }
    }
}
