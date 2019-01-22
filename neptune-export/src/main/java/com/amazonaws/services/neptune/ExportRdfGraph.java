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

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.DirectoryStructure;
import com.amazonaws.services.neptune.rdf.NeptuneSparqlClient;
import com.amazonaws.services.neptune.rdf.io.ExportRdfGraphJob;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;

import java.io.File;
import java.util.List;

@Command(name = "export-rdf", description = "Export RDF graph from Neptune to Turtle")
public class ExportRdfGraph implements Runnable {

    @Option(name = {"-e", "--endpoint"}, description = "Neptune endpoint(s) – supply multiple instance endpoints if you want to load balance requests across a cluster")
    @Required
    private List<String> endpoints;

    @Option(name = {"-p", "--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.USER})
    @Once
    private int port = 8182;

    @Option(name = {"-d", "--dir"}, description = "Root directory for output")
    @Required
    @Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File directory;

    @Option(name = {"-t", "--tag"}, description = "Directory prefix (optional)")
    @Once
    private String tag = "";

    @Override
    public void run() {

        try (Timer timer = new Timer();
             NeptuneSparqlClient client = NeptuneSparqlClient.create(endpoints, port)) {

            Directories directories = Directories.createFor(DirectoryStructure.Rdf, directory, tag);

            ExportRdfGraphJob job = new ExportRdfGraphJob(client, directories);
            job.execute();

            System.err.println();
            System.out.println(directories.directory());

        } catch (Exception e) {
            System.err.println("An error occurred while exporting from Neptune:");
            e.printStackTrace();
        }
    }
}
