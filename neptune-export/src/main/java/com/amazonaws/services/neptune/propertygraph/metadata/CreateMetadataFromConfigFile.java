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

package com.amazonaws.services.neptune.propertygraph.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class CreateMetadataFromConfigFile implements MetadataCommand {
    private final File configFilePath;

    public CreateMetadataFromConfigFile(File configFilePath) {
        this.configFilePath = configFilePath;
    }

    @Override
    public PropertiesMetadataCollection execute() throws IOException {
        JsonNode json = new ObjectMapper().readTree(configFilePath);
        return PropertiesMetadataCollection.fromJson(json);
    }
}
