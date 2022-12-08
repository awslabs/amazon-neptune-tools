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
import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.profiles.neptune_ml.Output;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TrainingDataWriterConfigV2;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TextFastTextFeatureTest {
    @Test
    public void shouldCreateFastTextFeatureConfigForNodeWithSuppliedValues() throws IOException {
        runTest("t1.json");
    }

    @Test
    public void shouldCreateFastTextFeatureConfigForEdgeWithSuppliedValues() throws IOException {
        runTest("t2.json");
    }

    @Test
    public void shouldAddWarningForUnknownLanguage() throws IOException {
        runTest("t3.json");
    }

    @Test
    public void shouldRaiseErrorIfLanguageMissing() throws IOException {
        try {
            runTest("t4.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'language' field for text_fasttext feature. Expected one of the following values: 'en', 'zh', 'hi', 'es', 'fr'.", e.getMessage());
        }
    }

    @Test
    public void shouldAllowMissingMaxLangthProperty() throws IOException {
        runTest("t5.json");
    }

    private void runTest(String jsonFile) throws IOException {
        JsonNode json = JsonFromResource.get(jsonFile, getClass());

        GraphSchema graphSchema = GraphSchema.fromJson(json.get("schema"));

        JsonNode expectedTrainingDataConfig = json.get("config");

        Collection<TrainingDataWriterConfigV2> overrideConfig = TrainingDataWriterConfigV2.fromJson(json.get("feature"), NeptuneMLSourceDataModel.PropertyGraph);

        Output output = new Output();

        new PropertyGraphTrainingDataConfigWriterV2(
                graphSchema,
                output.generator(),
                PropertyGraphTrainingDataConfigWriterV2.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                overrideConfig.iterator().next()).write();

        assertEquals(Output.format(expectedTrainingDataConfig), Output.format(output.allOutput()));
    }
}
