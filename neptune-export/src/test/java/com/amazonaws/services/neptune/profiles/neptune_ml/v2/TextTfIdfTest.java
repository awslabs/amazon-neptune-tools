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

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.profiles.neptune_ml.JsonFromResource;
import com.amazonaws.services.neptune.profiles.neptune_ml.Output;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TrainingDataWriterConfigV2;
import com.amazonaws.services.neptune.propertygraph.io.PrinterOptions;
import com.amazonaws.services.neptune.propertygraph.schema.GraphSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class TextTfIdfTest {

    @Test
    public void shouldCreateTfTidFeatureConfigWithSuppliedValues() throws IOException {
        runTest("t1.json");
    }

    @Test
    public void shouldThrowErrorIfNgramRangeIsMissing() throws IOException {
        try {
            runTest("t2.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'ngram_range' field for text_tfidf feature (Label: Person, Property: bio). Expected an array with 2 numeric values.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfNgramRangeHasOnlyOneElement() throws IOException {
        try {
            runTest("t3.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'ngram_range' field for text_tfidf feature (Label: Person, Property: bio). Expected an array with 2 numeric values.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfNgramRangeHasMoreThanTwoElements() throws IOException {
        try {
            runTest("t4.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'ngram_range' field for text_tfidf feature (Label: Person, Property: bio). Expected an array with 2 numeric values.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfNgramRangeHasNonNumericElements() throws IOException {
        try {
            runTest("t5.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'ngram_range' field for text_tfidf feature (Label: Person, Property: bio). Expected an array with 2 numeric values.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfMinDfIsMissing() throws IOException {
        try {
            runTest("t6.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'min_df' field for text_tfidf feature (Label: Person, Property: bio). Expected an integer value.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfMinDfIsNonNumeric() throws IOException {
        try {
            runTest("t7.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'min_df' field for text_tfidf feature (Label: Person, Property: bio). Expected an integer value.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfMaxFeaturesIsMissing() throws IOException {
        try {
            runTest("t8.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'max_features' field for text_tfidf feature (Label: Person, Property: bio). Expected an integer value.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorIfMaxFeaturesIsNonNumeric() throws IOException {
        try {
            runTest("t9.json");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Error parsing 'max_features' field for text_tfidf feature (Label: Person, Property: bio). Expected an integer value.", e.getMessage());
        }
    }

    @Test
    public void shouldCreateAutoFeatureConfigForMultiValueProperty() throws IOException {
        runTest("t10.json");
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
