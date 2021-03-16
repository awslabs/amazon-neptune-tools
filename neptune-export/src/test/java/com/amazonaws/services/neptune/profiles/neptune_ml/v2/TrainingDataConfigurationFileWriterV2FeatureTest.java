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

import com.amazonaws.services.neptune.profiles.neptune_ml.JsonFromResource;
import com.amazonaws.services.neptune.profiles.neptune_ml.Output;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TrainingDataConfigurationFileWriterV2FeatureTest {

    @Test
    public void shouldWriteVersionAndQueryEngine() throws IOException {
        runTest("t1_schema.json", "t1_training_config.json");
    }

    @Test
    public void shouldNodeAndEdgeObjectForEachFile() throws IOException {
        runTest("t2_schema.json", "t2_training_config.json");
    }

    @Test
    public void shouldAddAutoFeatureForSingleValueStringProperty() throws IOException {
        runTest("t3_schema.json", "t3_training_config.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorForMultiValueStringProperty() throws IOException {
        runTest("t4_schema.json", "t4_training_config.json");
    }

    @Test
    public void shouldAddNumericFeatureWithNormMinMaxAndMedianImputerForSingleValueIntegerProperty() throws IOException {
        runTest("t5_schema.json", "t5_training_config.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorAndMedianImputerForMultiValueIntegerProperty() throws IOException {
        runTest("t6_schema.json", "t6_training_config.json");
    }

    @Test
    public void shouldAddNumericFeatureWithNormMinMaxAndMedianImputerForSingleValueFloatProperty() throws IOException {
        runTest("t7_schema.json", "t7_training_config.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorAndMedianImputerForMultiValueFloatProperty() throws IOException {
        runTest("t8_schema.json", "t8_training_config.json");
    }

    @Test
    public void shouldAddDatetimeFeatureWithAllDatetimePartsForSingleValueDateProperty() throws IOException {
        runTest("t9_schema.json", "t9_training_config.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorForMultiValueDateProperty() throws IOException {
        runTest("t10_schema.json", "t10_training_config.json");
    }

    private void runTest(String schemaFile, String expectedTrainingConfigFile) throws IOException {
        GraphSchema graphSchema = GraphSchema.fromJson(JsonFromResource.get(schemaFile, getClass()));

        JsonNode expectedTrainingDataConfig = JsonFromResource.get(expectedTrainingConfigFile, getClass());

        Output output = new Output();

        new TrainingDataConfigurationFileWriterV2(
                graphSchema,
                output.generator(),
                TrainingDataConfigurationFileWriterV2.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS).write();

        assertEquals(Output.format(expectedTrainingDataConfig), Output.format(output.allOutput()));
    }
}