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

package com.amazonaws.services.neptune.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HttpResponse {

    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int status;
    private final String content;
    private final String contentType;

    public HttpResponse(int status, String content, String contentType) {
        this.status = status;
        this.content = content;
        this.contentType = contentType;
    }

    public int getStatus() {
        return status;
    }

    public String getContent() {
        return content;
    }

    public JsonNode getContentAsJson() throws IOException {
        if (contentType.equals(JSON_CONTENT_TYPE)) {
            return MAPPER.readTree(content);
        } else {
            throw new IllegalStateException("Content is not JSON: " + contentType);
        }
    }

    public <T> T getContentAsObject(Class<T> type) throws IOException {
        if (contentType.equals(JSON_CONTENT_TYPE)) {
            @SuppressWarnings("unchecked")
            T returnValue = (T) MAPPER.readerFor(type).readValue(content);
            return returnValue;
        } else {
            throw new IllegalStateException("Content is not JSON: " + contentType);
        }
    }

    public <T> Collection<T> getContentAsCollection(Class<T> type) throws IOException {
        if (contentType.equals(JSON_CONTENT_TYPE)) {

            ObjectReader reader = MAPPER.readerFor(type);
            List<T> results = new ArrayList<>();

            ArrayNode array = (ArrayNode) MAPPER.readTree(content);
            for (JsonNode node : array) {
                results.add(reader.readValue(node));
            }

            return results;
        } else {
            throw new IllegalStateException("Content is not JSON: " + contentType);
        }
    }
}
