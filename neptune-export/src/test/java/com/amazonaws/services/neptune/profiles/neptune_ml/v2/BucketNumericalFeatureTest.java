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

public class BucketNumericalFeatureTest {

    @Test
    public void shouldCreateBucketNumericalFeatureConfigWithSuppliedValues() throws IOException {
        runTest("t1_schema.json", "t1_override.json", "t1_training_config.json");
    }

    @Test
    public void shouldThrowErrorIfSeparatorSuppliedForBucketNumericalFeature() throws IOException {
        try {
            runTest("t2_schema.json", "t2_override.json", "t2_training_config.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid 'separator' field for bucket_numerical feature. Bucket numerical feature property cannot contain multiple values.", e.getMessage());
        }
    }

    private void runTest(String schemaFile, String overrideFile, String expectedTrainingConfigFile) throws IOException {
        GraphSchema graphSchema = GraphSchema.fromJson(JsonFromResource.get(schemaFile, getClass()));

        JsonNode expectedTrainingDataConfig = JsonFromResource.get(expectedTrainingConfigFile, getClass());

        Collection<TrainingDataWriterConfigV2> overrideConfig = TrainingDataWriterConfigV2.fromJson(JsonFromResource.get(overrideFile, getClass()));

        Output output = new Output();

        new TrainingDataConfigurationFileWriterV2(
                graphSchema,
                output.generator(),
                TrainingDataConfigurationFileWriterV2.COLUMN_NAME_WITHOUT_DATATYPE,
                PrinterOptions.NULL_OPTIONS,
                overrideConfig.iterator().next()).write();

        try {
            assertEquals(Output.format(expectedTrainingDataConfig), Output.format(output.allOutput()));
        } catch (Exception e) {
            throw e;
        }
    }

}
