package com.amazonaws.services.neptune.profiles.neptune_ml.v2.config;

import com.amazonaws.services.neptune.profiles.neptune_ml.NeptuneMLSourceDataModel;
import com.amazonaws.services.neptune.profiles.neptune_ml.JsonFromResource;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.*;

public class TrainingDataWriterConfigV2Test {

    @Test
    public void shouldCreateSingleConfig() throws IOException {
        JsonNode json = JsonFromResource.get("t1.json", getClass());

        Collection<TrainingDataWriterConfigV2> config = TrainingDataWriterConfigV2.fromJson(json.path("neptune_ml"), NeptuneMLSourceDataModel.PropertyGraph);

        assertEquals(1, config.size());
    }

    @Test
    public void shouldConfigForEachElementInArray() throws IOException {
        JsonNode json = JsonFromResource.get("t2.json", getClass());

        Collection<TrainingDataWriterConfigV2> config = TrainingDataWriterConfigV2.fromJson(json.path("neptune_ml"), NeptuneMLSourceDataModel.PropertyGraph);

        assertEquals(3, config.size());
    }

    @Test
    public void shouldConfigForEachElementInJobsArray() throws IOException {
        JsonNode json = JsonFromResource.get("t3.json", getClass());

        Collection<TrainingDataWriterConfigV2> config = TrainingDataWriterConfigV2.fromJson(json.path("neptune_ml"), NeptuneMLSourceDataModel.PropertyGraph);

        assertEquals(5, config.size());
    }

}