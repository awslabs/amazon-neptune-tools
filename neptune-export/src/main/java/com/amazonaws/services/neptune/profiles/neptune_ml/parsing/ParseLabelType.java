package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.fasterxml.jackson.databind.JsonNode;

public class ParseLabelType {

    private final String prefix;
    private final JsonNode json;

    public ParseLabelType(String prefix, JsonNode json) {
        this.prefix = prefix;
        this.json = json;
    }

    public String parseLabel() {
        if (json.has("type") && json.get("type").isTextual()) {
            String type = json.get("type").textValue();
            if  ( type.equals("regression")){
                return regressionLabel();
            }
        }

        return classLabel();
    }

    private String regressionLabel() {
        return String.format("%s_regression_label", prefix);
    }

    private String classLabel() {
        return String.format("%s_class_label", prefix);
    }
}
