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

import com.amazonaws.services.neptune.propertygraph.Scope;
import com.amazonaws.services.neptune.propertygraph.io.Format;
import com.amazonaws.services.neptune.propertygraph.io.Output;
import com.amazonaws.services.neptune.propertygraph.metadata.TokensOnly;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.AllowedValues;
import com.github.rvesse.airline.annotations.restrictions.Once;

import java.util.ArrayList;
import java.util.List;

public class NeptuneExportPropertyGraphBaseCommand extends NeptuneExportBaseCommand {

    @Option(name = {"-nl", "--node-label"}, description = "Labels of nodes to be exported (optional, default all labels)",
            arity = 1)
    protected List<String> nodeLabels = new ArrayList<>();

    @Option(name = {"-el", "--edge-label"}, description = "Labels of edges to be exported (optional, default all labels)",
            arity = 1)
    protected List<String> edgeLabels = new ArrayList<>();

    @Option(name = {"-r", "--range", "--range-size"}, description = "Number of items to fetch per request (optional)")
    @Once
    protected long rangeSize = -1;

    @Option(name = {"--limit"}, description = "Maximum number of items to export (optional)")
    @Once
    protected long limit = Long.MAX_VALUE;

    @Option(name = {"--skip"}, description = "Number of items to skip (optional)")
    @Once
    protected long skip = 0;

    @Option(name = {"-cn", "--concurrency"}, description = "Concurrency (optional)")
    @Once
    protected int concurrency = 1;

    @Option(name = {"-s", "--scope"}, description = "Scope (optional, default 'all')")
    @Once
    @AllowedValues(allowedValues = {"all", "nodes", "edges"})
    protected Scope scope = Scope.all;

    @Option(name = {"--format"}, description = "Output format (optional, default 'csv')")
    @Once
    @AllowedValues(allowedValues = {"csv", "csvNoHeaders", "json", "neptuneStreamsJson"})
    protected Format format = Format.csv;

    @Option(name = {"-o", "--output"}, description = "Output target (optional, default 'file')")
    @Once
    @AllowedValues(allowedValues = {"files", "stdout", "stream"})
    protected Output output = Output.files;

    @Option(name = {"--exclude-type-definitions"}, description = "Exclude type definitions from column headers (optional, default 'false')")
    @Once
    protected boolean excludeTypeDefinitions = false;

    @Option(name = {"--tokens-only"}, description = "Export tokens (~id, ~label) only (optional, default 'off')")
    @Once
    @AllowedValues(allowedValues = {"off", "nodes", "edges", "both"})
    protected TokensOnly tokensOnly = TokensOnly.off;

    @Option(name = {"--stream-name"}, description = "Name of an Amazon Kinesis Data Stream")
    @Once
    protected String streamName;

    @Option(name = {"--region"}, description = "AWS Region in which your Amazon Kinesis Data Stream is located")
    @Once
    protected String region;
}
