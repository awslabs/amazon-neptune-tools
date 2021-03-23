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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2;

import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.EdgeLabelTypeV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TrainingDataWriterConfigV2;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Collection;

public class RdfTrainingDataConfigWriter {

    private final Collection<String> filenames;
    private final JsonGenerator generator;
    private final TrainingDataWriterConfigV2 config;

    public RdfTrainingDataConfigWriter(Collection<String> filenames,
                                       JsonGenerator generator,
                                       TrainingDataWriterConfigV2 config) {
        this.filenames = filenames;
        this.generator = generator;
        this.config = config;
    }

    public void write() throws IOException {

        generator.writeStartObject();

        generator.writeStringField("version", "v2.0");
        generator.writeStringField("query_engine", "sparql");

        generator.writeObjectFieldStart("graph");
        writeRdfs();
        generator.writeEndObject();

        generator.writeEndObject();

        generator.flush();
    }

    private void writeRdfs() throws IOException {
        generator.writeArrayFieldStart("rdfs");

        for (String filename : filenames) {
            generator.writeStartObject();
            generator.writeStringField("file_name", filename);

            generator.writeObjectFieldStart("label");
            generator.writeStringField("task_type", EdgeLabelTypeV2.link_prediction.name());

            generator.writeArrayFieldStart("targets");

            generator.writeStartObject();
            generator.writeArrayFieldStart("split_rate");
            for (Double splitRate : config.defaultSplitRates()) {
                generator.writeNumber(splitRate);
            }
            generator.writeEndArray();
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();
            generator.writeEndObject();
        }

        generator.writeEndArray();
    }
}
