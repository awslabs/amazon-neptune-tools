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

import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.EdgeTaskTypeV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.LabelConfigV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.RdfTaskTypeV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TrainingDataWriterConfigV2;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class RdfTrainingDataConfigWriter {

    private final Collection<String> filenames;
    private final JsonGenerator generator;
    private final TrainingDataWriterConfigV2 config;
    private final Collection<String> warnings = new ArrayList<>();

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

        generator.writeArrayFieldStart("warnings");
        writeWarnings();
        generator.writeEndArray();

        generator.writeEndObject();

        generator.flush();
    }

    private void writeWarnings() throws IOException {
        for (String warning : warnings) {
            generator.writeString(warning);
        }
    }

    private void writeRdfs() throws IOException {
        generator.writeArrayFieldStart("rdfs");

        Collection<LabelConfigV2> classificationSpecifications = config.nodeConfig().getAllClassificationSpecifications();

        if (classificationSpecifications.isEmpty()) {
            for (String filename : filenames) {
                generator.writeStartObject();
                generator.writeStringField("file_name", filename);

                generator.writeObjectFieldStart("label");
                generator.writeStringField("task_type", EdgeTaskTypeV2.link_prediction.name());

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
        } else {

            for (RdfTaskTypeV2 taskType : RdfTaskTypeV2.values()) {
                List<LabelConfigV2> taskSpecificConfigs = classificationSpecifications.stream().filter(c -> c.taskType().equals(taskType.name())).collect(Collectors.toList());

                if (taskSpecificConfigs.isEmpty()) {
                    continue;
                }

                if (taskType == RdfTaskTypeV2.link_prediction) {
                    for (String filename : filenames) {
                        generator.writeStartObject();
                        generator.writeStringField("file_name", filename);

                        generator.writeObjectFieldStart("label");
                        generator.writeStringField("task_type", taskType.name());

                        generator.writeArrayFieldStart("targets");

                        for (LabelConfigV2 taskSpecificConfig : taskSpecificConfigs) {
                            generator.writeStartObject();

                            if (StringUtils.isNotEmpty(taskSpecificConfig.subject())) {
                                generator.writeStringField("subject", taskSpecificConfig.subject());
                            } else {
                                warnings.add("'subject' field is missing for link_prediction task, so all edges will be treated as the training target.");
                            }

                            if (StringUtils.isNotEmpty(taskSpecificConfig.property())) {
                                generator.writeStringField("predicate", taskSpecificConfig.property());
                            }else {
                                warnings.add("'predicate' field is missing for link_prediction task, so all edges will be treated as the training target.");
                            }

                            if (StringUtils.isNotEmpty(taskSpecificConfig.object())) {
                                generator.writeStringField("object", taskSpecificConfig.object());
                            }else {
                                warnings.add("'object' field is missing for link_prediction task, so all edges will be treated as the training target.");
                            }

                            generator.writeArrayFieldStart("split_rate");
                            for (Double splitRate : taskSpecificConfig.splitRates()) {
                                generator.writeNumber(splitRate);
                            }
                            generator.writeEndArray();
                            generator.writeEndObject();
                        }

                        generator.writeEndArray();
                        generator.writeEndObject();
                        generator.writeEndObject();
                    }
                } else {
                    for (String filename : filenames) {
                        generator.writeStartObject();
                        generator.writeStringField("file_name", filename);

                        generator.writeObjectFieldStart("label");
                        generator.writeStringField("task_type", taskType.name());

                        generator.writeArrayFieldStart("targets");

                        for (LabelConfigV2 taskSpecificConfig : taskSpecificConfigs) {
                            generator.writeStartObject();
                            generator.writeStringField("node", taskSpecificConfig.label().labelsAsString());
                            String property = taskSpecificConfig.property();

                            if (StringUtils.isNotEmpty(property)){
                                generator.writeStringField("predicate", property);
                            } else {
                                warnings.add(String.format("'predicate' field is missing for %s task. If the target nodes have more than one predicate defining the target node feature, the training task will fail with an error.", taskType));
                            }

                            generator.writeArrayFieldStart("split_rate");
                            for (Double splitRate : taskSpecificConfig.splitRates()) {
                                generator.writeNumber(splitRate);
                            }
                            generator.writeEndArray();
                            generator.writeEndObject();
                        }

                        generator.writeEndArray();
                        generator.writeEndObject();
                        generator.writeEndObject();
                    }
                }
            }
        }


        generator.writeEndArray();
    }
}
