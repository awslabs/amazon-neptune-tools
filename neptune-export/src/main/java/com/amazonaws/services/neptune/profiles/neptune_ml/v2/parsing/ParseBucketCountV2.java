package com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing;

import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ErrorMessageHelper;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.fasterxml.jackson.databind.JsonNode;

public class ParseBucketCountV2 {

    private final JsonNode json;
    private final ParsingContext context;

    public ParseBucketCountV2(JsonNode json, ParsingContext context) {
        this.json = json;
        this.context = context;
    }

    public int parseBucketCount() {
        if (json.has("bucket_cnt") && json.path("bucket_cnt").isInt()) {
            return json.path("bucket_cnt").asInt();
        } else if (json.has("num_buckets") && json.path("num_buckets").isInt()) {
            return json.path("num_buckets").asInt();
        }  else {
            throw ErrorMessageHelper.errorParsingField("bucket_cnt", context, "an integer");
        }
    }
}
