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

import com.amazonaws.services.neptune.propertygraph.io.Jsonizable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class NamedQueriesCollection implements Jsonizable {

    public static NamedQueriesCollection fromJson(JsonNode json) {
        List<NamedQueries> collection = new ArrayList<>();

        for (JsonNode jsonNode : json) {
            collection.add(NamedQueries.fromJson(jsonNode));
        }

        return new NamedQueriesCollection(collection);
    }


    private final Collection<NamedQueries> namedQueries;

    public NamedQueriesCollection(Collection<NamedQueries> namedQueries) {
        this.namedQueries = namedQueries;
    }

    public Collection<NamedQuery> flatten() {
        List<NamedQuery> queries = new ArrayList<>();
        namedQueries.forEach(q -> q.addTo(queries));
        return queries;
    }

    public Collection<String> names(){
        return namedQueries.stream().map(NamedQueries::name).collect(Collectors.toList());
    }

    @Override
    public JsonNode toJson() {
        ArrayNode json = JsonNodeFactory.instance.arrayNode();

        for (NamedQueries queries : namedQueries) {

            ObjectNode queriesNode = JsonNodeFactory.instance.objectNode();
            ArrayNode arrayNode = queries.toJson();

            queriesNode.put("name", queries.name());
            queriesNode.set("queries", arrayNode);

            json.add(queriesNode);
        }

        return json;
    }
}
