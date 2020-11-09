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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.services.neptune.io.CommandWriter;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;

import java.io.IOException;
import java.net.URI;

public class ExportPropertyGraphFromConfigModule {

    @Option(name = {"-c", "--config-file"}, description = "Path to JSON schema config file (file path, or 'https' or 's3' URI).")
    @RequireOnlyOne(tag = "configFile or config")
    private URI configFile;

    @Option(name = {"--config"}, description = "JSON schema for property graph.")
    @RequireOnlyOne(tag = "configFile or config")
    private String configJson;

    public GraphSchema graphSchema() throws IOException {

        if (configFile != null) {

            JsonResource<GraphSchema> configFileResource = new JsonResource<>(
                    "Config file",
                    configFile,
                    GraphSchema.class);

            return configFileResource.get();
        } else {
            return GraphSchema.fromJson(new ObjectMapper().readTree(configJson));
        }
    }

    public void writeResourcePathAsMessage(CommandWriter writer) {
        if (configFile == null) {
            return;
        }

        writer.writeMessage("Config file : " + configFile.toString());
    }
}
