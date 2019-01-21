/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NamedQueries {

    public static NamedQueries fromJson(JsonNode json) {
        String name = json.path("name").textValue();
        ArrayNode queries = (ArrayNode) json.path("queries");

        List<String> collection = new ArrayList<>();

        for (JsonNode query : queries) {
            collection.add(query.textValue());
        }

        return new NamedQueries(name, collection);
    }

    private final String name;
    private final Collection<String> queries;

    public NamedQueries(String name, Collection<String> queries) {
        this.name = name;
        this.queries = queries;
    }

    public String name() {
        return name;
    }

    public Collection<String> queries() {
        return queries;
    }

    public void addTo(Collection<NamedQuery> namedQueries) {
        for (String query : queries) {
            namedQueries.add(new NamedQuery(name, query));
        }
    }

    public ArrayNode toJson() {
        ArrayNode json = JsonNodeFactory.instance.arrayNode();

        for (String query : queries) {
            json.add(query);
        }

        return json;
    }
}
