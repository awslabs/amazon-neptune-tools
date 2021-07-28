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
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.FeatureEncodingFlag;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PropertyGraphTrainingDataConfigWriterV2FeatureTest {

    @Test
    public void shouldWriteVersionAndQueryEngine() throws IOException {
        runTest("t1.json");
    }

    @Test
    public void shouldAddNodeAndEdgeObjectForEachFile() throws IOException {
        runTest("t2.json");
    }

    @Test
    public void shouldAddAutoFeatureForSingleValueStringProperty() throws IOException {
        runTest("t3.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorForMultiValueStringProperty() throws IOException {
        runTest("t4.json");
    }

    @Test
    public void shouldAddNumericFeatureWithNormMinMaxAndMedianImputerForSingleValueIntegerProperty() throws IOException {
        runTest("t5.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorAndMedianImputerForMultiValueIntegerProperty() throws IOException {
        runTest("t6.json");
    }

    @Test
    public void shouldAddNumericFeatureWithNormMinMaxAndMedianImputerForSingleValueFloatProperty() throws IOException {
        runTest("t7.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorAndMedianImputerForMultiValueFloatProperty() throws IOException {
        runTest("t8.json");
    }

    @Test
    public void shouldAddDatetimeFeatureWithAllDatetimePartsForSingleValueDateProperty() throws IOException {
        runTest("t9.json");
    }

    @Test
    public void shouldAddAutoFeatureWithSeparatorForMultiValueDateProperty() throws IOException {
        runTest("t10.json");
    }

    private void runTest(String jsonFile) throws IOException {
        JsonNode json = JsonFromResource.get(jsonFile, getClass());

        GraphSchema graphSchema = GraphSchema.fromJson(json.get("schema"));

        JsonNode expectedTrainingDataConfig = json.get("config");

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV2(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV2.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS).write();

        assertEquals(Output.format(expectedTrainingDataConfig), Output.format(output.allOutput()));
    }
}