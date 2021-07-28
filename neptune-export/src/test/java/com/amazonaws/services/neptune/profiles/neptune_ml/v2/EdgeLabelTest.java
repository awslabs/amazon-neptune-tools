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
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TrainingDataWriterConfigV2;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EdgeLabelTest {

    @Test
    public void shouldCreateLabelForPropertyWithSpecifiedConfigValues() throws IOException {
        runTest("t1.json");
    }

    @Test
    public void shouldSupplyDefaultSplitRateIfSplitRateNotSpecified() throws IOException {
        runTest("t2.json");
    }

    @Test
    public void shouldUseTopLevelDefaultSplitRateIfSpecified() throws IOException {
        runTest("t3.json");
    }

    @Test
    public void shouldAllowEmptyPropertyForLinkPrediction() throws IOException {
        runTest("t4.json");
    }

    @Test
    public void shouldAllowMissingPropertyForLinkPrediction() throws IOException {
        runTest("t5.json");
    }

    @Test
    public void shouldThrowExceptionIfEmptyPropertyAndNotLinkPrediction() throws IOException {
        try {
            runTest("t6.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Missing or empty 'property' field for edge regression specification (Label: [Person, knows, Person]).", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfMissingPropertyAndNotLinkPrediction() throws IOException {
        try {
            runTest("t7.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Missing or empty 'property' field for edge regression specification (Label: [Person, knows, Person]).", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfUnrecognisedLabelType() throws IOException {
        try {
            runTest("t8.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid 'type' value for edge label (Label: [Person, knows, Person], Property: ): 'invalid'. Valid values are: 'classification', 'regression', 'link_prediction'.", e.getMessage());
        }
    }

    @Test
    public void shouldAllowComplexToAndFromLabels() throws IOException {
        runTest("t9.json");
    }

    private void runTest(String jsonFile) throws IOException {
        JsonNode json = JsonFromResource.get(jsonFile, getClass());

        GraphSchema graphSchema = GraphSchema.fromJson(json.get("schema"));

        JsonNode expectedTrainingDataConfig = json.get("config");

        Collection<TrainingDataWriterConfigV2> overrideConfig = TrainingDataWriterConfigV2.fromJson(json.get("label"));

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
