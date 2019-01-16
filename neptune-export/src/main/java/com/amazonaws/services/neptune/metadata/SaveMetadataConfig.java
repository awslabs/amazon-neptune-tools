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

package com.amazonaws.services.neptune.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.*;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SaveMetadataConfig {
    private final PropertiesMetadataCollection metadataCollection;
    private final Path configFilePath;

    public SaveMetadataConfig(PropertiesMetadataCollection metadataCollection, Path configFilePath) {
        this.metadataCollection = metadataCollection;
        this.configFilePath = configFilePath;
    }

    public void execute() throws IOException {
        File file = configFilePath.toFile();
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF_8))) {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            writer.write(objectWriter.writeValueAsString(metadataCollection.toJson()));
        }
    }
}
