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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.rdf.*;
import com.amazonaws.services.neptune.rdf.io.*;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.Once;

public class RdfExportScopeModule {

    @Option(name = {"--rdf-export-scope"}, description = "Export scope (optional, default 'graph').")
    @Once
    @AllowedEnumValues(RdfExportScope.class)
    private RdfExportScope scope = RdfExportScope.graph;

    public ExportRdfJob createJob(NeptuneSparqlClient client, RdfTargetConfig targetConfig){
        if (scope == RdfExportScope.graph){
            return new ExportRdfGraphJob(client, targetConfig);
        } else if (scope == RdfExportScope.edges){
            return new ExportRdfEdgesJob(client, targetConfig);
        }
        throw new IllegalStateException(String .format("Unknown export scope: %s", scope));
    }

    public String scope(){
        return scope.name();
    }
}
