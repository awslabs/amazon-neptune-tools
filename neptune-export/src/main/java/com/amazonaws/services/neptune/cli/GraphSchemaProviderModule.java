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

import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.MutuallyExclusiveWith;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.URI;

public class GraphSchemaProviderModule {

    @Option(name = {"-c", "--config-file", "--filter-config-file"}, description = "Path to JSON schema config file (file path, or 'https' or 's3' URI).")
    @MutuallyExclusiveWith(tag = "configFile or config")
    private URI configFile;

    @Option(name = {"--config", "--filter"}, description = "JSON schema for property graph.")
    @MutuallyExclusiveWith(tag = "configFile or config")
    private String configJson;

    private final boolean configIsMandatory;

    public GraphSchemaProviderModule(boolean configIsMandatory) {
        this.configIsMandatory = configIsMandatory;
    }

    public GraphSchema graphSchema() throws IOException {

        if (configFile != null) {

            JsonResource<GraphSchema> configFileResource = new JsonResource<>(
                    "Config file",
                    configFile,
                    GraphSchema.class);

            return configFileResource.get();

        } else {

            if (StringUtils.isEmpty(configJson)){
                if (configIsMandatory){
                    throw new IllegalStateException("You must supply either a configuration file URI or inline configuration JSON");
                }
                return new GraphSchema();
            }

            return GraphSchema.fromJson(new ObjectMapper().readTree(configJson));
        }
    }
}
